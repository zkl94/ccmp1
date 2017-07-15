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

    cout << "calling constructor for MP1Node" << endl;
    this->tobedeleted = new std::map<int, int>;
    this->tobedeleted->clear();
    cout << "tobedeleted size is " << this->tobedeleted->size() << endl;
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

    // add self to memberlist
    int id = 0;
    short port;
    memcpy(&id, &(memberNode->addr.addr[0]), sizeof(int));
    memcpy(&port, &(memberNode->addr.addr[4]), sizeof(short));
    MemberListEntry *newEntry = new MemberListEntry(id, port, 0, par->getcurrtime());
    memberNode->memberList.push_back(*newEntry);

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
    emulNet->ENcleanup();
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

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

void MP1Node::logNodeAddWrapper(Member *memberNode, int id, short port) {
    Address address;
    string _address = to_string(id) + ":" + to_string(port);
    address = Address(_address);
    log->logNodeAdd(&memberNode->addr, &address);
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

    // for introduction:
    // two message types: JOINREQ, JOINREP
    // 1. when introducer receives JOINREQ, add the node to member list, and return JOINREP with cluster member list
    // 2. when cluster member receives JOINREP, add the cluster member list

    // for membership:
    // one message type: MEMBERLIST
    // 1. when receive MEMBERLIST, update you own member list accordingly

    cout << "handling message" <<endl;

    MsgTypes msgType = ((MessageHdr *)data)->msgType;

    switch (msgType) {
        case JOINREQ:
            handleJOINREQ(env, data, size);
            break;
        case JOINREP:
            handleJOINREP(env, data, size);
            break;
        case MEMBERLIST:
            handleMEMBERLIST(env, data, size);
            break;
        default:
            cout << "unknown message type" <<endl;
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

    // newly added node have 0 heartbeat
    MemberListEntry *newEntry = new MemberListEntry(id, port, 0, par->getcurrtime());
    memberNode->memberList.push_back(*newEntry);

    logNodeAddWrapper(memberNode, id, port);

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
        // only send heartbeat, timestamp is set by receivers with par->getcurrtime()
    }

    #ifdef DEBUG_JOINREQ
        cout << "currentOffset = " << currentOffset << " JOINREPMsgSize = " << JOINREPMsgSize << endl;
    #endif
    assert(currentOffset == JOINREPMsgSize);

    string _address = to_string(id) + ":" + to_string(port);
    Address address = Address(_address);
    emulNet->ENsend(&memberNode->addr, &address, (char *)JOINREPMsg, JOINREPMsgSize);
    #ifdef DEBUG_JOINREQ
        cout << "JOINREP successfully sent" <<endl;
    #endif
}

#define DEBUG_JOINREQ

void MP1Node::handleJOINREP(void *env, char *data, int size) {
    #ifdef DEBUG_JOINREQ
        cout << "just got a join response with size " << size << endl;
    #endif

    // // use the returned member list to initiate the member list of self
    // int entry_num = (size - 4)/ (6+8);
    // int currentOffset = 4;
    // char addr[6];
    // int id = 0;
    // short port;
    // long heartbeat = 0;
    // for (int i = 0; i < entry_num; ++i)
    // {
    //     // addr
    //     MessageHdr *msg = (MessageHdr *)data;
    //     memcpy(&addr, (char *)(msg)+currentOffset, 6);
    //     currentOffset += 6;
    //     memcpy(&id, &addr[0], sizeof(int));
    //     memcpy(&port, &addr[4], sizeof(short));
        
    //     // heartbeat
    //     memcpy(&heartbeat, (char *)(msg)+currentOffset, 8);
    //     currentOffset += 8;

    //     updateEntry(memberNode, id, port, heartbeat);
    // }
    handleMEMBERLIST(env, data, size);

    memberNode->inGroup = true;
}

void MP1Node::updateEntry(Member *memberNode, int id, short port, long heartbeat) {
    // if this node (with id) is already in tobedeleted list, just ignore it
    auto alreadyintobedeletedlist = this->tobedeleted->count(id);
    if (alreadyintobedeletedlist != 0) {
        // already in tobedeleted list, ignore the update
        return;
    }

    // if it is not to be deleted, then update normally
    bool found = false;
    for (MemberListEntry memberListEntry: memberNode->memberList) {

        if (memberListEntry.getid() == id && memberListEntry.getport() == port) {
            found = true;
            // the incoming heartbeat is more latest
            if (memberListEntry.getheartbeat() < heartbeat) {
                memberListEntry.setheartbeat(heartbeat);
                memberListEntry.settimestamp(par->getcurrtime());
            }
            break;
        } else {
            continue;
        }
    }
    if (!found) {
        MemberListEntry *newEntry = new MemberListEntry(id, port, heartbeat, par->getcurrtime());
        memberNode->memberList.push_back(*newEntry);

        logNodeAddWrapper(memberNode, id, port);
    }
}

// void MP1Node::updateEntryHEARTBEAT(Member *memberNode, int id, short port, long heartbeat) {
//     // if the node marks another node as failed and already added it to tobedeleted dict
//     // and after some time it receives heartbeat from that node, then it should remove the
//     // node from the tobedeleted dict and put it back to the memberList

//     bool found = false;
//     for (MemberListEntry memberListEntry: memberNode->memberList) {

//         if (memberListEntry.getid() == id && memberListEntry.getport() == port) {
//             found = true;
//             if (memberListEntry.getheartbeat() < heartbeat) {
//                 memberListEntry.setheartbeat(heartbeat);
//             }
//             break;
//         } else {
//             continue;
//         }
//     }
//     if (!found) {
//         // @TODO: timestamp may be needed
//         MemberListEntry *newEntry = new MemberListEntry(id, port, heartbeat, 0);
//         memberNode->memberList.push_back(*newEntry);

//         logNodeAddWrapper(memberNode, id, port);
//     }
// }

// #define DEBUG_HEARTBEAT

// void MP1Node::handleHEARTBEAT(void *env, char *data, int size) {
//     #ifdef DEBUG_HEARTBEAT
//         cout << "just got a heartbeat" << endl;
//     #endif

//     // use the addr and heartbeat to update the entry
//     char addr[6];
//     MessageHdr *msg = (MessageHdr *)data;
//     memcpy(&addr, (char *)(msg)+4, 6);
//     int id = 0;
//     short port;
//     memcpy(&id, &addr[0], sizeof(int));
//     memcpy(&port, &addr[4], sizeof(short));
//     long heartbeat = 0;
//     memcpy(&heartbeat, (char *)(msg)+10, 8);

//     updateEntryHEARTBEAT(memberNode, id, port, heartbeat);
// }

#define DEBUG_MEMBERLIST

void MP1Node::handleMEMBERLIST(void *env, char *data, int size) {
    // #ifdef DEBUG_MEMBERLIST
    //     cout << "just got a member list" << endl;
    // #endif

    int entry_num = (size - 4)/ (6+8);
    int currentOffset = 4;
    char addr[6];
    int id = 0;
    short port;
    long heartbeat = 0;

    // for every addr and heartbeat in the incoming member list
    for (int i = 0; i < entry_num; ++i)
    {
        // extract id and port
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

// check if the timestamp is outdated
bool isEntryInvalid(Member *memberNode, MemberListEntry& memberListEntry) {
    long entry_timestamp = memberListEntry.gettimestamp();
    long node_timestamp = memberNode->memberList[0].gettimestamp();

    if (node_timestamp - entry_timestamp > TFAIL) {
        return true;
    }
    return false;
}

void MP1Node::updateToBeDeletedList() {
    cout << "enter updateToBeDeletedList" << endl;
    cout << "the size of tobedeleted is " << this->tobedeleted->empty();

    std::map<int, int>::iterator it = this->tobedeleted->begin();
    
    while(it != this->tobedeleted->end()) {
        cout << it->second << " in updateToBeDeletedList" << endl;
        it->second = it->second + 1;
        if (it->second >= TREMOVE) {
            // log the removal
            Address address;
            string _address = to_string(it->first) + ":" + to_string(0);
            address = Address(_address);
            log->logNodeRemove(&memberNode->addr, &address);

            // remove from to-be-deleted map; can be re-added by membership protocol now
            it = this->tobedeleted->erase(it);
        } else {
            ++it;
        }
    }
    cout << "out of updateToBeDeletedList" << endl;
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {

    // first implement all to all heartbeat
    // 0. update self heartbeat and tiemstamp
    // 1. Check if any node hasn't responded within a timeout period and then delete the nodes
    // 2. Propagate your membership list to every other member (MEMBERLIST)

    // update self heartbeat
    memberNode->heartbeat++;

    // update the entry of self in self memberlist
    // memberNode->memberList[0] is self entry because it is done in introduceSelfToGroup
    memberNode->memberList[0].setheartbeat(memberNode->heartbeat);
    memberNode->memberList[0].settimestamp(par->getcurrtime());

    cout << "before checking isEntryInvalid" <<endl;
    memberNode->myPos = memberNode->memberList.begin();
    while (memberNode->myPos != memberNode->memberList.end()) {
        if(isEntryInvalid(memberNode, *memberNode->myPos)) {
            int id = memberNode->myPos->getid();
            short port = memberNode->myPos->getport();
            Address address;
            string _address = to_string(id) + ":" + to_string(port);
            address = Address(_address);

            cout << "found invalid entry with id " << id <<endl;

            // @TODO: instead of deleting right away, add it to to-be-deleted dict
            // to-be-deleted dict: node id: times
            // log->logNodeRemove(&memberNode->addr, &address);
            // free((MemberListEntry *)&(*memberNode->myPos));
            // cout << "the size of tobedeleted is " << tobedeleted->size();
            this->tobedeleted->insert(std::make_pair(id, 0));

            memberNode->myPos = memberNode->memberList.erase(memberNode->myPos);
        } else {
            ++memberNode->myPos;
        }
    }

    cout << "after checking isEntryInvalid" <<endl;

    this->updateToBeDeletedList();

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

    // send / construct two kinds of messages
    for (MemberListEntry memberListEntry: memberNode->memberList) {
        Address address;
        string _address = to_string(memberListEntry.getid()) + ":" + to_string(memberListEntry.getport());
        address = Address(_address);

        // add to MEMBERLIST
        memcpy((char *)(memberListMsg) + currentOffset, &address.addr, sizeof(address.addr));
        currentOffset += sizeof(address.addr);
        memcpy((char *)(memberListMsg) + currentOffset, &memberListEntry.heartbeat, sizeof(long));
        currentOffset += sizeof(long);
    }

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
