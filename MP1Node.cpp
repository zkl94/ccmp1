/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"
#include <algorithm> // for remove_if
#include <functional> // for unary_function

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    // join address is the address of the introducer
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
	int id = *(int*)(&memberNode->addr.addr);
	int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
        // size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long);
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        // memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));
        memcpy((char *)(msg+1) + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

        free(msg);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   /*
    * Your code goes here
    */
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
	/*
	 * Your code goes here
	 */

    // for introduction:
    // two message types: JOINREQ, JOINREP
    // 1. when introducer receives JOINREQ, return JOINREP (cluster member list)
    // 2. when cluster member receives JOINREP, add the cluster member list

    // for membership:
    // two message types: HEARTBEAT, ECHO, MEMBERLIST
    // 1. when receive HEARTBEAT, send back ECHO
    // 2. when receive ECHO, update member list entry for that node
    // 3. when receive MEMBERLIST, update you own member list accordingly


}

#define HEARTBEAT_THRESHOLD 10

// this is a single threaded application
long node_heartbeat = 0;

// check if the heartbeat is outdated
bool isEntryInvalid(MemberListEntry& memberListEntry) {
    long entry_heartbeat = memberListEntry.getheartbeat();
    // some threahold
    if (node_heartbeat - entry_heartbeat > HEARTBEAT_THRESHOLD) {
        return true;
    }
    return false;
}

struct remove_entry : public std::unary_function<MemberListEntry&, bool> {
    bool operator() (MemberListEntry& memberListEntry)
    {
        return isEntryInvalid(memberListEntry);
    }
};

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {

	/*
	 * Your code goes here
	 */

    // first implement all to all heartbeat
    // 0. update self heartbeat
    // 1. Check if any node hasn't responded within a timeout period and then delete the nodes
    // 2. send heartbeats to all other members (HEARTBEAT)
    // 3. Propagate your membership list to every other member (MEMBERLIST)

    // update self heartbeat
    memberNode->heartbeat++;
    node_heartbeat = memberNode->heartbeat;

    // Check if any node hasn't responded within a timeout period and then delete the nodes
    getMemberNode()->memberList.erase(std::remove_if(getMemberNode()->memberList.begin(), getMemberNode()->memberList.end(), remove_entry()),
                getMemberNode()->memberList.end());

    // construct MEMBERLIST message
    int entry_num = getMemberNode()->memberList.size();
    int currentOffset = 0; 
    // 6 for char addr[6]
    size_t memberListMsgSize = sizeof(MessageHdr) + (6 + sizeof(long)) * entry_num;
    MessageHdr *memberListMsg;
    memberListMsg = (MessageHdr *) malloc(memberListMsgSize * sizeof(char));
    memberListMsg->msgType = MEMBERLIST;
    // for the message type offset
    currentOffset += 1;

    // construct HEARTBEAT message
    MessageHdr *heartbeatMsg;
    // 6 for char addr[6]
    size_t heartbeatMsgSize = sizeof(MessageHdr) + 6 + sizeof(long);
    heartbeatMsg = (MessageHdr *) malloc(heartbeatMsgSize * sizeof(char));

    heartbeatMsg->msgType = HEARTBEAT;
    memcpy((char *)(heartbeatMsg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
    memcpy((char *)(heartbeatMsg+1) + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

    // send / construct two kinds of messages
    for (MemberListEntry memberListEntry: getMemberNode()->memberList) {
        Address address;
        // send heartbeats to all other members
        string _address = to_string(memberListEntry.getid()) + ":" + to_string(memberListEntry.getport());
        address = Address(_address);
        emulNet->ENsend(&memberNode->addr, &address, (char *)heartbeatMsg, heartbeatMsgSize);

        // add to MEMBERLIST
        memcpy((char *)(memberListMsg+currentOffset), &address.addr, sizeof(address.addr));
        currentOffset += sizeof(address.addr);
        memcpy((char *)(heartbeatMsg+currentOffset), &memberListEntry.heartbeat, sizeof(long));
        currentOffset += sizeof(long);
    }
    free(heartbeatMsg);

    // Propagate your membership list
    for (MemberListEntry memberListEntry: getMemberNode()->memberList) {
        Address address;
        string _address = to_string(memberListEntry.getid()) + ":" + to_string(memberListEntry.getport());
        address = Address(_address);
        emulNet->ENsend(&memberNode->addr, &address, (char *)memberListMsg, memberListMsgSize);
    }

    return;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

// 1.0.0.0:0
    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}
