#include "ping.h"
#include <pthread.h>
#include <net/if.h>

extern char interface_name[];
extern int interface_flag;

Ping::Ping() 
{
    m_maxPacketSize = 4;
    m_datalen = 56;
    m_nsend = 0;
    m_nreceived = 0;
    m_icmp_seq = 0;
    memset(m_sendpacket, 0, sizeof(m_sendpacket));
}

/*校验和算法*/
unsigned short Ping::getChksum(unsigned short *addr,int len)
{   
    int nleft=len;
    int sum=0;
    unsigned short *w=addr;
    unsigned short answer=0;

    /*把ICMP报头二进制数据以2字节为单位累加起来*/
    while(nleft>1)
    {   
        sum+=*w++;
        nleft-=2;
    }
    /*若ICMP报头为奇数个字节，会剩下最后一字节。把最后一个字节视为一个2字节数据的高字节，这个2字节数据的低字节为0，继续累加*/
    if( nleft==1)
    {   
        *(unsigned char *)(&answer)=*(unsigned char *)w;
        sum+=answer;
    }
    sum=(sum>>16)+(sum&0xffff);
    sum+=(sum>>16);
    answer=~sum;
    return answer;
}

/*设置ICMP报头*/
int Ping::packIcmp(int pack_no, struct icmp* icmp)
{   
    int packsize;
    struct icmp *picmp;
    struct timeval *tval;

    picmp = icmp;
    picmp->icmp_type=ICMP_ECHO;
    picmp->icmp_code=0;
    picmp->icmp_cksum=0;
    picmp->icmp_seq=pack_no;
    picmp->icmp_id= m_pid;
    packsize= 8 + m_datalen;
    tval= (struct timeval *)icmp->icmp_data;
    gettimeofday(tval,NULL);    /*记录发送时间*/
    picmp->icmp_cksum=getChksum((unsigned short *)icmp,packsize); /*校验算法*/
    return packsize;
}

/*剥去ICMP报头*/
bool Ping::unpackIcmp(char *buf,int len, struct IcmpEchoReply *icmpEchoReply)
{   
    int iphdrlen;
    struct ip *ip;
    struct icmp *icmp;
    struct timeval *tvsend, tvrecv, tvresult;
    double rtt;

    ip = (struct ip *)buf;
    iphdrlen = ip->ip_hl << 2;    /*求ip报头长度,即ip报头的长度标志乘4*/
    icmp = (struct icmp *)(buf + iphdrlen);  /*越过ip报头,指向ICMP报头*/
    len -= iphdrlen;            /*ICMP报头及ICMP数据报的总长度*/
    if(len < 8)                /*小于ICMP报头长度则不合理*/
    {   
        printf("ICMP packets\'s length is less than 8\n");
        return false;
    }
    /*确保所接收的是我所发的的ICMP的回应*/
    if( (icmp->icmp_type==ICMP_ECHOREPLY) && (icmp->icmp_id == m_pid) )
    {   
        tvsend=(struct timeval *)icmp->icmp_data;
        gettimeofday(&tvrecv,NULL);  /*记录接收时间*/
        tvresult = tvSub2(tvrecv, *tvsend);  /*接收和发送的时间差*/
        rtt=tvresult.tv_sec*1000 + tvresult.tv_usec/(double)1000;  /*以毫秒为单位计算rtt*/
        icmpEchoReply->rtt = rtt;
        icmpEchoReply->icmpSeq = icmp->icmp_seq;
        icmpEchoReply->ipTtl = ip->ip_ttl;
        icmpEchoReply->icmpLen = len;
        return true;
    }
    else {
        return false;
    }
}

/*两个timeval结构相减*/
//struct timeval Ping::tvSub(struct timeval timeval1,struct timeval timeval2)
//{ 	
//    struct timeval result;
//    result = timeval1;
//    if (result.tv_usec < timeval2.tv_usec < 0)
//    {    
//        --result.tv_sec;
//        result.tv_usec += 1000000;
//    }
//    result.tv_sec -= timeval2.tv_sec;
//    return result;
//}
/*两个timeval结构相减*/
// yuanhong fix: 
// 第一个时间减去第二个时间。
// 最终返回一个有符号的timeval结果，无论timeval的字段正负，
// 如果第一个确实大于第二个,那么最终的结果值一定是正确的。
// 作者这个函数处理有问题,故改之
struct timeval Ping::tvSub2(struct timeval tv1,struct timeval tv2)
{ 	
    struct timeval res;
    res.tv_sec  = tv1.tv_sec  - tv2.tv_sec;
    res.tv_usec = tv1.tv_usec - tv2.tv_usec;
    //printf("sec %ld,  usec %ld\n", res.tv_sec, res.tv_usec);
    return res;
}

/*发送三个ICMP报文*/
bool Ping::sendPacket()
{   
    size_t packetsize;
    while( m_nsend < m_maxPacketSize)
    {   
        m_nsend++;
        m_icmp_seq++;
        packetsize = packIcmp(m_icmp_seq, (struct icmp*)m_sendpacket); /*设置ICMP报头*/

        if(sendto(m_sockfd,m_sendpacket, packetsize, 0, (struct sockaddr *) &m_dest_addr, sizeof(m_dest_addr)) < 0  )
        {   
            perror("sendto error");
            continue;
        }
    }
    return true;
}

pthread_mutex_t net_mutex = PTHREAD_MUTEX_INITIALIZER;

/*接收所有ICMP报文*/
bool Ping::recvPacket(PingResult &pingResult)
{       
    int len;
    extern int errno;
    struct IcmpEchoReply icmpEchoReply;
    int maxfds = m_sockfd + 1;
    int nfd  = 0;
    fd_set rset;     
    FD_ZERO(&rset);
    socklen_t fromlen = sizeof(m_from_addr);
    struct timeval timeout;
    timeout.tv_sec = 3;
    timeout.tv_usec = 0;

    for (int recvCount = 0; recvCount < m_maxPacketSize; recvCount++) 
    {   
        //printf("begin recv\n");
        FD_SET(m_sockfd, &rset);
        if ((nfd = select(maxfds, &rset, NULL, NULL, &timeout)) == -1) {             
            perror("select error");	
            continue;   
        }
        if (nfd == 0) {
            //recv time out 
            //printf("request timeout\n");
            icmpEchoReply.isReply = false;
            pingResult.icmpEchoReplys.push_back(icmpEchoReply);
            continue;
        }
        if (FD_ISSET(m_sockfd, &rset)) {
            if( (len = recvfrom(m_sockfd, m_recvpacket,sizeof(m_recvpacket),0, (struct sockaddr *)&m_from_addr,&fromlen)) <0)
            {	   
                if(errno==EINTR)
                    continue;
                perror("recvfrom error");
                continue;
            }
            pthread_mutex_lock(&net_mutex);
            icmpEchoReply.fromAddr = inet_ntoa(m_from_addr.sin_addr) ;
            pthread_mutex_unlock(&net_mutex);
            if (icmpEchoReply.fromAddr != pingResult.ip) {
                //printf("invalid address, discard\n");
                //retry again
                recvCount--;
                continue;
            }
        }
        if(unpackIcmp(m_recvpacket, len, &icmpEchoReply)==-1) {
            //retry again
            recvCount--;
            continue;
        }
        icmpEchoReply.isReply = true;
        pingResult.icmpEchoReplys.push_back(icmpEchoReply);
        {
            if (icmpEchoReply.rtt > pingResult.max) pingResult.max = icmpEchoReply.rtt; 
            if (icmpEchoReply.rtt < pingResult.min) pingResult.min = icmpEchoReply.rtt; 
            pingResult.tot += icmpEchoReply.rtt;
        }
        m_nreceived++;
    }
    return true;
}

bool Ping::getsockaddr(const char * hostOrIp, struct sockaddr_in* sockaddr) {
    struct sockaddr_in dest_addr;
    bzero(&dest_addr,sizeof(dest_addr));
    dest_addr.sin_family=AF_INET;

    int ret = inet_pton(AF_INET, hostOrIp, &dest_addr.sin_addr.s_addr);
    switch (ret) {
        case 0:
        case -1:
            return false;
        default:
            break;
    }
    dest_addr.sin_family = AF_INET; 
    *sockaddr = dest_addr;
    return true;
}
bool Ping::ping(std::string host, PingResult& pingResult) 
{
    return ping(host, 1, pingResult);
}


bool Ping::ping(std::string host, int count, PingResult& pingResult) 
{
    struct protoent *protocol;
    int size = 50 * 1024;
    IcmpEchoReply icmpEchoReply;

    m_nsend = 0;
    m_nreceived = 0;
    pingResult.icmpEchoReplys.clear();
    m_maxPacketSize = count;
    m_pid = getpid();

    pingResult.dataLen = m_datalen;

    pthread_mutex_lock(&net_mutex);
    if( (protocol = getprotobyname("icmp") )==NULL)
    {   
        pthread_mutex_unlock(&net_mutex);
        perror("getprotobyname");
        return false;
    }
    pthread_mutex_unlock(&net_mutex);
    /*生成使用ICMP的原始套接字,这种套接字只有root才能生成*/
    if( (m_sockfd=socket(AF_INET,SOCK_RAW,protocol->p_proto) )<0)
    {       
        //perror("socket error");
        extern int errno;
        pingResult.error = strerror(errno);
        return false;
    }
    /*扩大套接字接收缓冲区到50K这样做主要为了减小接收缓冲区溢出的
      的可能性,若无意中ping一个广播地址或多播地址,将会引来大量应答*/
    setsockopt(m_sockfd,SOL_SOCKET,SO_RCVBUF,&size,sizeof(size) );

    if (interface_flag) {
        struct ifreq interface;
        memset(&interface, 0, sizeof(struct ifreq));
        strncpy(interface.ifr_ifrn.ifrn_name, interface_name, IFNAMSIZ);
        if (setsockopt(m_sockfd, SOL_SOCKET, SO_BINDTODEVICE, &interface, sizeof(interface)) < 0) {
            char buff[1024];
            snprintf(buff, sizeof(buff),"setsockopt SO_BINDTODEVICE %s error %d:%s", interface_name, errno, strerror(errno));
            pingResult.error = buff;
            close(m_sockfd);
            return false;
        }
    }

    /*获取main的进程id,用于设置ICMP的标志符*/
    if (!getsockaddr(host.c_str(), &m_dest_addr)) {
        pingResult.error = "错误ip地址: " + host;
        close(m_sockfd);
        return false;
    }
    pthread_mutex_lock(&net_mutex);
    pingResult.ip = inet_ntoa(m_dest_addr.sin_addr);
    pthread_mutex_unlock(&net_mutex);
    sendPacket();  /*发送所有ICMP报文*/
    recvPacket(pingResult);  /*接收所有ICMP报文*/
    pingResult.nsend = m_nsend;
    pingResult.nreceived = m_nreceived;
    close(m_sockfd);
    return true;
}
