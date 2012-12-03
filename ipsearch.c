/** 
* IP Search client for QQWry.Dat 
*/ 
#include <stdio.h> 
#include <stdlib.h> 
#include <string.h> 
#include <iconv.h>
 
typedef struct ip_info { 
    char ip[16]; 
    char *country;     //国家 
    char *area;        //地区 
    unsigned char start_ip[4]; // For debug开始IP地址 
    unsigned char end_ip[4];   // For debug结束IP地址 
    char *mode;        // For debug 模式 
} IP_INFO; 

unsigned long ip_str2dec(char *ip_in) //把字符串去掉点转化成10进制数 
{ 
    char *ip = strdup(ip_in);
    unsigned long ip_dec = 0;
    unsigned long check = 0;
    char *p; 
    int i; 

    for(i=0; i<3; i++) { 
        p = strrchr(ip, '.');
        check = atoi(p + 1);
        if (check > 256) {
            /* reset to ZERO */
            check = 0;
        }
        ip_dec |= check << (8 * i);
        *p = '\0'; 
    }
    ip_dec |= atoi(ip) << (8 * i);
    free(ip); 
    
    return ip_dec; 
} 
 
unsigned long ip_arr2dec_r(unsigned char *ip_arr) //把四字节的ip地址组合成一个数 
{
    unsigned long ip_dec = 0; 
    int i; 

    for(i=0; i<4; i++) { 
        ip_dec |= ip_arr[i] << (8*i); 
    } 
    return ip_dec; 
} 
 
unsigned long get_long_addr3(unsigned char *buf); 
char* get_string_by_addr(long addr, FILE *fp); 
IP_INFO *get_ip_by_index(unsigned long index_addr, FILE *fp); 
char* get_area(unsigned char* buffer, FILE *fp); 
unsigned long search_ip(char *ip_in, FILE *fp); 
 
unsigned long get_long_addr3(unsigned char *buf)//转化地址 
{ 
    unsigned long addr = 0; 
    
    addr = buf[0] | buf[1] << 8 | buf[2] << 16; 
    
    return addr; 
} 
 
char* get_string_by_addr(long addr, FILE *fp)//通过绝对地址获取字符串 
{ 
    unsigned char buffer[1024]; 

    fseek(fp, addr, SEEK_SET); 
    fread(buffer, 1024, 1, fp); 

    return strdup( (char*) buffer); 
}

//通过索引获取IP地址相应信息，如国家和地区
IP_INFO *get_ip_by_index(unsigned long index_addr, FILE *fp)
{ 
    unsigned char buffer[1024]; 
    IP_INFO *ipinfo = (IP_INFO *)malloc(sizeof(IP_INFO)); 
    unsigned long country_addr; 
    unsigned long record_addr = 0;
    unsigned char addr[3]={0,}; 

    if (!ipinfo)
        return NULL;
    
    fseek(fp, index_addr, SEEK_SET); 
    fread(ipinfo->start_ip, 4, 1, fp);
    
    fread(addr, 3, 1, fp); 
    
    record_addr = get_long_addr3(addr);
 
 
    fseek(fp, record_addr, SEEK_SET); 
    fread(ipinfo->end_ip, 4, 1, fp); 
    fread(buffer, 1024, 1, fp); 
 
    if (buffer[0] == 1)//模式1 
    { 
        country_addr = get_long_addr3(buffer + 1); 
        fseek(fp, country_addr, SEEK_SET); 
        fread(buffer, 1024, 1, fp); 
        
        if (buffer[0] == 2) { 
            ipinfo->country = get_string_by_addr(get_long_addr3(buffer + 1), fp); 
            ipinfo->area = get_area(buffer + 4, fp); 
            ipinfo->mode = "1 + 2"; 
        } else { 
            ipinfo->country = get_string_by_addr(country_addr, fp); 
            ipinfo->area = get_area(buffer + strlen(ipinfo->country) + 1, fp); 
            ipinfo->mode = "1 + D"; 
        } 
    } else if (buffer[0] == 2)//模式2 
    { 
        ipinfo->country = get_string_by_addr(get_long_addr3(buffer + 1), fp); 
        ipinfo->area = get_area(buffer + 4, fp); 
        ipinfo->mode = "2 + D"; 
        
    } else//其它，即IP记录的最简单形式 
    { 
        ipinfo->country = strdup((char*) buffer); 
        ipinfo->area = get_area(buffer + strlen(ipinfo->country) + 1, fp); 
        ipinfo->mode = "D + D"; 
    }

    return ipinfo; 
} 
 
char* get_area(unsigned char* buffer, FILE *fp)//获取区域信息 
{ 
    if (buffer[0] == 1 || buffer[0] == 2) { 
        return get_string_by_addr(get_long_addr3(buffer + 1), fp);//通过绝对偏移获得 
    } else { 
        return strdup((char*) buffer); 
    } 
} 

unsigned long search_ip(char *ip_in, FILE *fp)//查找索引区，获取所在区域首地址 
{ 
    unsigned long index_start, index_end, lo, hi, i, ip_i, ip_dest; 
    unsigned char ip_arr[4]; 
    
    fseek(fp, 0, SEEK_SET);   //SEEK_SET=0 
    fread(&index_start, 4, 1, fp); 
    fread(&index_end, 4, 1, fp); 
    lo = 0; 
    hi = (index_end - index_start) / 7; //索引数 
    ip_dest = ip_str2dec(ip_in); 
    
    while(lo<=hi)         //用二分法查询索引表 
    { 
        i = (lo + hi) / 2; 
        fseek(fp, index_start + i * 7, SEEK_SET); 
        fread(ip_arr, 4, 1, fp); 
        ip_i = ip_arr2dec_r(ip_arr); 
        if (ip_i == ip_dest) 
            return index_start + i * 7;//在索引表中找到匹配地址就返回地址（在整个文件中的位置） 有问题,有可能找不到 
        else if (ip_i < ip_dest) { 
            lo = i + 1; 
            if(lo>=hi) {
                return index_start + (i+1) * 7; 
            }
        } else { 
            hi = i - 1; 
            if(lo>=hi) {
                return index_start + (i-1) * 7; 
            }
        }
    }
    return 0;       //如果找不到就返回0 
} 
 
int code_convert(char *from_charset, char *to_charset, char *inbuf, size_t inlen,
                 char *outbuf, size_t outlen)
{
    iconv_t cd;
    int rc;
    char **pin = &inbuf;
    char **pout = &outbuf;
    
    cd = iconv_open(to_charset,from_charset);
    if (cd==0) return -1;
    memset(outbuf,0,outlen);
    if (iconv(cd,pin,&inlen,pout,&outlen)==-1) return -1;
    iconv_close(cd);
    return 0;
}

int g2u(char *inbuf,size_t inlen,char *outbuf,size_t outlen)
{
    return code_convert("gbk", "utf-8", inbuf, inlen, outbuf, outlen);
}
 
int main(int argc, char* argv[]) 
{ 
    FILE *fp; 
    fp = fopen("./conf/qqwry.dat", "rb"); 
    if(fp==NULL) printf("不能打开QQWry.Dat!\n"); 

    //char *ip = "159.226.41.88";
    char *ip = "202.113.16.117";
    //char *ip = "159.226.251.13";
    if (argc > 1) {
        ip = argv[1];
    }

    unsigned long  ipaddress=search_ip(ip, fp); 
    if(ipaddress==0) {printf("找不到要查询的ip!\n");getchar();return 0;} 
    IP_INFO *ipinfo = get_ip_by_index(ipaddress, fp); 
    char address[1000]={0}; 
    char out1[100], out2[100];
    g2u(ipinfo->country, strlen(ipinfo->country), out1, 100);
    g2u(ipinfo->area, strlen(ipinfo->area), out2, 100);
    sprintf(address,"%s/%s\n",out1, out2); 
    printf("ip:%s => address:%s\n", ip, address); 
    
    free(ipinfo); 
    
    fclose(fp); 

    return 0; 
}
