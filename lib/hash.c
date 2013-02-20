/*
 **************************************************************************
 *                                                                        *
 *          General Purpose Hash Function Algorithms Library              *
 *                                                                        *
 * Author: Arash Partow - 2002                                            *
 * URL: http://www.partow.net                                             *
 * URL: http://www.partow.net/programming/hashfunctions/index.html        *
 *                                                                        *
 * Copyright notice:                                                      *
 * Free use of the General Purpose Hash Function Algorithms Library is    *
 * permitted under the guidelines and in accordance with the most current *
 * version of the Common Public License.                                  *
 * http://www.opensource.org/licenses/cpl.php                             *
 *                                                                        *
 **************************************************************************
*/

/* Murmurhash from http://sites.google.com/site/murmurhash/
 *
 * All code is released to public domain. For business purposes, Murmurhash is
 * under the MIT license.
 */

/**
 * Other codes written by Ma Can following the GPL
 *
 * Copyright (c) 2009 Ma Can <ml.macana@gmail.com>
 *                           <macan@ncic.ac.cn>
 *
 * Armed with EMACS.
 * Time-stamp: <2010-07-10 14:04:11 macan>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 */

/* BEGIN OF General Hash Functions */
static inline unsigned int RSHash(char* str, unsigned int len)
{
   unsigned int b    = 378551;
   unsigned int a    = 63689;
   unsigned int hash = 0;
   unsigned int i    = 0;

   for(i = 0; i < len; str++, i++)
   {
      hash = hash * a + (*str);
      a    = a * b;
   }

   return hash;
}
/* End Of RS Hash Function */


static inline unsigned int JSHash(char* str, unsigned int len)
{
   unsigned int hash = 1315423911;
   unsigned int i    = 0;

   for(i = 0; i < len; str++, i++)
   {
      hash ^= ((hash << 5) + (*str) + (hash >> 2));
   }

   return hash;
}
/* End Of JS Hash Function */


static inline unsigned int PJWHash(char* str, unsigned int len)
{
   const unsigned int BitsInUnsignedInt = (unsigned int)(sizeof(unsigned int) * 8);
   const unsigned int ThreeQuarters     = (unsigned int)((BitsInUnsignedInt  * 3) / 4);
   const unsigned int OneEighth         = (unsigned int)(BitsInUnsignedInt / 8);
   const unsigned int HighBits          = (unsigned int)(0xFFFFFFFF) << (BitsInUnsignedInt - OneEighth);
   unsigned int hash              = 0;
   unsigned int test              = 0;
   unsigned int i                 = 0;

   for(i = 0; i < len; str++, i++)
   {
      hash = (hash << OneEighth) + (*str);

      if((test = hash & HighBits)  != 0)
      {
         hash = (( hash ^ (test >> ThreeQuarters)) & (~HighBits));
      }
   }

   return hash;
}
/* End Of  P. J. Weinberger Hash Function */


static inline unsigned int ELFHash(char* str, unsigned int len)
{
   unsigned int hash = 0;
   unsigned int x    = 0;
   unsigned int i    = 0;

   for(i = 0; i < len; str++, i++)
   {
      hash = (hash << 4) + (*str);
      if((x = hash & 0xF0000000L) != 0)
      {
         hash ^= (x >> 24);
      }
      hash &= ~x;
   }

   return hash;
}
/* End Of ELF Hash Function */


static inline unsigned int BKDRHash(char* str, unsigned int len)
{
   unsigned int seed = 131; /* 31 131 1313 13131 131313 etc.. */
   unsigned int hash = 0;
   unsigned int i    = 0;

   for(i = 0; i < len; str++, i++)
   {
      hash = (hash * seed) + (*str);
   }

   return hash;
}
/* End Of BKDR Hash Function */


static inline unsigned int SDBMHash(char* str, unsigned int len)
{
   unsigned int hash = 0;
   unsigned int i    = 0;

   for(i = 0; i < len; str++, i++)
   {
      hash = (*str) + (hash << 6) + (hash << 16) - hash;
   }

   return hash;
}
/* End Of SDBM Hash Function */


static inline unsigned int DJBHash(char* str, unsigned int len)
{
   unsigned int hash = 5381;
   unsigned int i    = 0;

   for(i = 0; i < len; str++, i++)
   {
      hash = ((hash << 5) + hash) + (*str);
   }

   return hash;
}
/* End Of DJB Hash Function */


static inline unsigned int DEKHash(char* str, unsigned int len)
{
   unsigned int hash = len;
   unsigned int i    = 0;

   for(i = 0; i < len; str++, i++)
   {
      hash = ((hash << 5) ^ (hash >> 27)) ^ (*str);
   }
   return hash;
}
/* End Of DEK Hash Function */


static inline unsigned int BPHash(char* str, unsigned int len)
{
   unsigned int hash = 0;
   unsigned int i    = 0;
   for(i = 0; i < len; str++, i++)
   {
      hash = hash << 7 ^ (*str);
   }

   return hash;
}
/* End Of BP Hash Function */


static inline unsigned int FNVHash(char* str, unsigned int len)
{
   const unsigned int fnv_prime = 0x811C9DC5;
   unsigned int hash      = 0;
   unsigned int i         = 0;

   for(i = 0; i < len; str++, i++)
   {
      hash *= fnv_prime;
      hash ^= (*str);
   }

   return hash;
}
/* End Of FNV Hash Function */


static inline unsigned int APHash(char* str, unsigned int len)
{
   unsigned int hash = 0xAAAAAAAA;
   unsigned int i    = 0;

   for(i = 0; i < len; str++, i++)
   {
       hash ^= ((i & 1) == 0) ? (  (hash <<  7) ^ ((*str) * (hash >> 3))) :
           (~(((hash << 11) + (*str)) ^ (hash >> 5)));
   }

   return hash;
}
/* End Of AP Hash Function */
/* END OF General Hash Functions */

static inline
u64 __murmurhash2_64a(const void *key, int len, u64 seed)
{
    const u64 m = 0xc6a4a7935bd1e995;
    const int r = 47;

    u64 h = seed ^ (len * m);

    const u64 *data = (const u64 *)key;
    const u64 *end = data + (len/8);

    while (data != end) {
        u64 k = *data++;

        k *= m;
        k ^= k >> r;
        k *= m;

        h ^= k;
        h *= m;
    }

    const unsigned char *data2 = (const unsigned char *)data;

    switch (len & 7) {
    case 7: h ^= ((u64)data2[6]) << 48;
    case 6: h ^= ((u64)data2[5]) << 40;
    case 5: h ^= ((u64)data2[4]) << 32;
    case 4: h ^= ((u64)data2[3]) << 24;
    case 3: h ^= ((u64)data2[2]) << 16;
    case 2: h ^= ((u64)data2[1]) << 8;
    case 1: h ^= ((u64)data2[0]);
        h *= m;
    }

    h ^= h >> r;
    h *= m;
    h ^= h >> r;

    return h;
}

static inline u64 rotl64 ( u64 x, u8 r )
{
  return (x << r) | (x >> (64 - r));
}

#define ROTL64(x,y)     rotl64(x,y)

#define BIG_CONSTANT(x) (x##LLU)

//-----------------------------------------------------------------------------
// Finalization mix - force all bits of a hash block to avalanche

static inline u64 fmix ( u64 k )
{
  k ^= k >> 33;
  k *= BIG_CONSTANT(0xff51afd7ed558ccd);
  k ^= k >> 33;
  k *= BIG_CONSTANT(0xc4ceb9fe1a85ec53);
  k ^= k >> 33;

  return k;
}

static inline void MurmurHash3_x64_128 ( const void * key, const int len,
                                         const u32 seed, void * out )
{
  const u8 * data = (const u8 *)key;
  const int nblocks = len / 16;
  int i;

  u64 h1 = seed;
  u64 h2 = seed;

  const u64 c1 = BIG_CONSTANT(0x87c37b91114253d5);
  const u64 c2 = BIG_CONSTANT(0x4cf5ad432745937f);

  //----------
  // body

  const u64 * blocks = (const u64 *)(data);

  for(i = 0; i < nblocks; i++)
  {
      u64 k1 = blocks[i*2];
      u64 k2 = blocks[i*2+1];

      k1 *= c1; k1  = ROTL64(k1,31); k1 *= c2; h1 ^= k1;
      
      h1 = ROTL64(h1,27); h1 += h2; h1 = h1*5+0x52dce729;
      
      k2 *= c2; k2  = ROTL64(k2,33); k2 *= c1; h2 ^= k2;
      
      h2 = ROTL64(h2,31); h2 += h1; h2 = h2*5+0x38495ab5;
  }

  //----------
  // tail

  const u8 * tail = (const u8*)(data + nblocks*16);

  u64 k1 = 0;
  u64 k2 = 0;

  switch(len & 15)
  {
  case 15: k2 ^= (u64)(tail[14]) << 48;
  case 14: k2 ^= (u64)(tail[13]) << 40;
  case 13: k2 ^= (u64)(tail[12]) << 32;
  case 12: k2 ^= (u64)(tail[11]) << 24;
  case 11: k2 ^= (u64)(tail[10]) << 16;
  case 10: k2 ^= (u64)(tail[ 9]) << 8;
  case  9: k2 ^= (u64)(tail[ 8]) << 0;
           k2 *= c2; k2  = ROTL64(k2,33); k2 *= c1; h2 ^= k2;

  case  8: k1 ^= (u64)(tail[ 7]) << 56;
  case  7: k1 ^= (u64)(tail[ 6]) << 48;
  case  6: k1 ^= (u64)(tail[ 5]) << 40;
  case  5: k1 ^= (u64)(tail[ 4]) << 32;
  case  4: k1 ^= (u64)(tail[ 3]) << 24;
  case  3: k1 ^= (u64)(tail[ 2]) << 16;
  case  2: k1 ^= (u64)(tail[ 1]) << 8;
  case  1: k1 ^= (u64)(tail[ 0]) << 0;
      k1 *= c1; k1  = ROTL64(k1,31); k1 *= c2; h1 ^= k1;
  };

  //----------
  // finalization

  h1 ^= len; h2 ^= len;

  h1 += h2;
  h2 += h1;

  h1 = fmix(h1);
  h2 = fmix(h2);

  h1 += h2;
  h2 += h1;

  ((u64*)out)[0] = h1;
  ((u64*)out)[1] = h2;
}

static inline
u64 hvfs_hash(char *key, int len)
{
    
#if 0
    return __murmurhash2_64a(key, len, 0xaf89245feaedfe07);
#else
    u64 r[2];
    MurmurHash3_x64_128 (key, len, 0xfaedfe07, r);
    return r[0] ^ r[1];
#endif
}
