/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"
#include <algorithm> // for remove_if
#include <functional> // for unary_function
#include <cassert>

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

    Address *my_addr = &this->memberNode->addr;

    // Self booting routines
    if( initThisNode(my_addr) == -1 ) {
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
int MP1Node::initThisNode(Address *my_addr) {
	/*
	 * This function is partially implemented and may require changes
	 */
	// int id = *(int*)(&my_addr->addr);
	// int port = *(short*)(&my_addr->addr[4]);

    // cout << "initing node" << endl;

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
    // cout << "introducing myself to the group" << endl;

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

        // member node in group only after receiving JOINREP

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
    return 0;
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

    #ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "checking messages...");
    #endif

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
        #ifdef DEBUGLOG
            log->LOG(&memberNode->addr, "found a message...");
        #endif

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
bool MP1Node::recvCallBack(void *env, char *data, int size) {
	/*
	 * Your code goes here
	 */

    // env is fucking useless

    // for introduction:
    // two message types: JOINREQ, JOINREP
    // 1. when introducer receives JOINREQ, add the node to member list, and return JOINREP with cluster member list
    // 2. when cluster member receives JOINREP, add the cluster member list

    // for membership:
    // two message types: HEARTBEAT, MEMBERLIST
    // 1. when receive HEARTBEAT, update member list entry for that node
    // 3. when receive MEMBERLIST, update you own member list accordingly

    #ifdef DEBUGLOG
        log->LOG(&(memberNode->addr), "recvCallBack");
    #endif

    MsgTypes msgType = ((MessageHdr *)data)->msgType;

    switch (msgType) {
        case JOINREQ:
            handleJOINREQ(env, data, size);
            break;
        case JOINREP:
            handleJOINREP(env, data, size);
            break;
        case HEARTBEAT:
            handleHEARTBEAT(env, data, size);
            break;
        case MEMBERLIST:
            handleMEMBERLIST(env, data, size);
            break;
        default:
            break;
    }

    return false;

}

#define DEBUG_JOINREQ

void MP1Node::handleJOINREQ(void *env, char *data, int size) {
    #ifdef DEBUG_JOINREQ
        cout << "received a join request" << endl;
    #endif

    // when introducer receives JOINREQ, add the node to member list, and return JOINREP with cluster member list
    char addr[6];
    // msg: sizeof(MessageHdr) + 6 + sizeof(long)
    MessageHdr *msg = (MessageHdr *)data;
    memcpy(&addr, (char *)(msg)+4, 6);
    int id = 0;
    short port;
    memcpy(&id, &addr[0], sizeof(int));
    memcpy(&port, &addr[4], sizeof(short));

    #ifdef DEBUG_JOINREQ
        cout << "id = " << id << "; port = " << port << endl;
    #endif

    #ifdef DEBUG_JOINREQ
        cout << "before adding member list entry, there are " << memberNode->memberList.size() << " entries" << endl;
    #endif

    MemberListEntry *newEntry = new MemberListEntry(id, port);
    memberNode->memberList.push_back(*newEntry);

    #ifdef DEBUG_JOINREQ
        cout << "after adding member list entry, there are " << memberNode->memberList.size() << " entries" << endl;
    #endif

    // return JOINREP with cluster member list
    int entry_num = memberNode->memberList.size();
    int currentOffset = 0; 
    // 6 for char addr[6]
    size_t JOINREPMsgSize = sizeof(MessageHdr) + (6 + sizeof(long)) * entry_num;
    MessageHdr *JOINREPMsg;
    JOINREPMsg = (MessageHdr *) malloc(JOINREPMsgSize * sizeof(char));
    JOINREPMsg->msgType = JOINREP;
    // for the message type offset
    currentOffset += 4;

    for (MemberListEntry memberListEntry: memberNode->memberList) {
        Address address;
        string _address = to_string(memberListEntry.getid()) + ":" + to_string(memberListEntry.getport());
        address = Address(_address);
        // add to JOINREPMsg
        memcpy((char *)(JOINREPMsg) + currentOffset, &address.addr, sizeof(address.addr));
        currentOffset += sizeof(address.addr);
        memcpy((char *)(JOINREPMsg) + currentOffset, &memberListEntry.heartbeat, sizeof(long));
        currentOffset += sizeof(long);
    }

    #ifdef DEBUG_JOINREQ
        cout << "currentOffset = " << currentOffset << " JOINREPMsgSize = " << JOINREPMsgSize << endl;
    #endif
    assert(currentOffset == JOINREPMsgSize);

    string _address = to_string(id) + ":" + to_string(port);
    Address address = Address(_address);
    emulNet->ENsend(&memberNode->addr, &address, (char *)JOINREPMsg, JOINREPMsgSize);
}

#define DEBUG_JOINREQ

void MP1Node::handleJOINREP(void *env, char *data, int size) {
    #ifdef DEBUG_JOINREQ
        cout << "just got a join response with size " << size << endl;
    #endif

    // use the returned member list to initiate the member list of self
    int entry_num = (size - 4)/ (6+8);
    int currentOffset = 4;
    char addr[6];
    int id = 0;
    short port;
    long heartbeat = 0;
    for (int i = 0; i < entry_num; ++i)
    {
        // addr
        MessageHdr *msg = (MessageHdr *)data;
        memcpy(&addr, (char *)(msg)+currentOffset, 6);
        currentOffset += 6;
        memcpy(&id, &addr[0], sizeof(int));
        memcpy(&port, &addr[4], sizeof(short));
        
        // heartbeat
        memcpy(&heartbeat, (char *)(msg)+currentOffset, 8);
        currentOffset += 8;
        // @TODO: timestamp may be needed
        MemberListEntry *newEntry = new MemberListEntry(id, port, heartbeat, 0);
        memberNode->memberList.push_back(*newEntry);
    }

    memberNode->inGroup = true;
}

void updateEntry(Member *memberNode, int id, short port, long heartbeat) {
    bool found = false;
    for (MemberListEntry memberListEntry: memberNode->memberList) {

        if (memberListEntry.getid() == id && memberListEntry.getport() == port) {
            found = true;
            if (memberListEntry.getheartbeat() < heartbeat) {
                memberListEntry.setheartbeat(heartbeat);
            }
            break;
        } else {
            continue;
        }
    }
    if (!found) {
        // @TODO: timestamp may be needed
        MemberListEntry *newEntry = new MemberListEntry(id, port, heartbeat, 0);
        memberNode->memberList.push_back(*newEntry);
    }
}

#define DEBUG_HEARTBEAT

void MP1Node::handleHEARTBEAT(void *env, char *data, int size) {
    #ifdef DEBUG_HEARTBEAT
        cout << "just got a heartbeat" << endl;
    #endif

    // use the addr and heartbeat to update the entry
    char addr[6];
    MessageHdr *msg = (MessageHdr *)data;
    memcpy(&addr, (char *)(msg)+4, 6);
    int id = 0;
    short port;
    memcpy(&id, &addr[0], sizeof(int));
    memcpy(&port, &addr[4], sizeof(short));
    long heartbeat = 0;
    memcpy(&heartbeat, (char *)(msg)+10, 8);

    updateEntry(memberNode, id, port, heartbeat);
}

#define DEBUG_MEMBERLIST

void MP1Node::handleMEMBERLIST(void *env, char *data, int size) {
    #ifdef DEBUG_MEMBERLIST
        cout << "just got a member list" << endl;
    #endif

    // for every addr and heartbeat in the incoming member list, do the same as handleHEARTBEATs
    int entry_num = (size - 4)/ (6+8);
    int currentOffset = 4;
    char addr[6];
    int id = 0;
    short port;
    long heartbeat = 0;
    for (int i = 0; i < entry_num; ++i)
    {
        // addr
        MessageHdr *msg = (MessageHdr *)data;
        memcpy(&addr, (char *)(msg)+currentOffset, 6);
        currentOffset += 6;
        memcpy(&id, &addr[0], sizeof(int));
        memcpy(&port, &addr[4], sizeof(short));
        
        // heartbeat
        memcpy(&heartbeat, (char *)(msg)+currentOffset, 8);
        currentOffset += 8;

        updateEntry(memberNode, id, port, heartbeat);
    }
}

#define HEARTBEAT_THRESHOLD 10

// this is a single threaded application
long node_heartbeat = 0;

// check if the heartbeat is outdated
bool isEntryInvalid(MemberListEntry& memberListEntry) {
    // can't be based on
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
    getMemberNode()->memberList.erase(std::remove_if(memberNode->memberList.begin(), memberNode->memberList.end(), remove_entry()),
                memberNode->memberList.end());
    // @TODO: does remove_if trigger destructor

    // construct MEMBERLIST message
    int entry_num = getMemberNode()->memberList.size();
    int currentOffset = 0; 
    // 6 for char addr[6]
    size_t memberListMsgSize = sizeof(MessageHdr) + (6 + sizeof(long)) * entry_num;
    MessageHdr *memberListMsg;
    memberListMsg = (MessageHdr *) malloc(memberListMsgSize * sizeof(char));
    memberListMsg->msgType = MEMBERLIST;
    // for the message type offset (int)
    currentOffset += 4;

    // construct HEARTBEAT message
    MessageHdr *heartbeatMsg;
    // 6 for char addr[6]
    size_t heartbeatMsgSize = sizeof(MessageHdr) + 6 + sizeof(long);
    heartbeatMsg = (MessageHdr *) malloc(heartbeatMsgSize * sizeof(char));

    heartbeatMsg->msgType = HEARTBEAT;
    memcpy((char *)(heartbeatMsg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
    memcpy((char *)(heartbeatMsg+1) + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

    // send / construct two kinds of messages
    for (MemberListEntry memberListEntry: memberNode->memberList) {
        Address address;
        // send heartbeats to all other members
        string _address = to_string(memberListEntry.getid()) + ":" + to_string(memberListEntry.getport());
        address = Address(_address);
        emulNet->ENsend(&memberNode->addr, &address, (char *)heartbeatMsg, heartbeatMsgSize);

        // add to MEMBERLIST
        memcpy((char *)(memberListMsg) + currentOffset, &address.addr, sizeof(address.addr));
        currentOffset += sizeof(address.addr);
        memcpy((char *)(memberListMsg) + currentOffset, &memberListEntry.heartbeat, sizeof(long));
        currentOffset += sizeof(long);
    }
    free(heartbeatMsg);

    assert(currentOffset == memberListMsgSize);

    // Propagate your membership list
    for (MemberListEntry memberListEntry: memberNode->memberList) {
        Address address;
        string _address = to_string(memberListEntry.getid()) + ":" + to_string(memberListEntry.getport());
        address = Address(_address);
        emulNet->ENsend(&memberNode->addr, &address, (char *)memberListMsg, memberListMsgSize);
    }

    free(memberListMsg);

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
