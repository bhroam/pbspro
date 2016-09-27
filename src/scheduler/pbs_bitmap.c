/*
 * Copyright (C) 1994-2016 Altair Engineering, Inc.
 * For more information, contact Altair at www.altair.com.
 *  
 * This file is part of the PBS Professional ("PBS Pro") software.
 * 
 * Open Source License Information:
 *  
 * PBS Pro is free software. You can redistribute it and/or modify it under the
 * terms of the GNU Affero General Public License as published by the Free 
 * Software Foundation, either version 3 of the License, or (at your option) any 
 * later version.
 *  
 * PBS Pro is distributed in the hope that it will be useful, but WITHOUT ANY 
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Affero General Public License for more details.
 *  
 * You should have received a copy of the GNU Affero General Public License along 
 * with this program.  If not, see <http://www.gnu.org/licenses/>.
 *  
 * Commercial License Information: 
 * 
 * The PBS Pro software is licensed under the terms of the GNU Affero General 
 * Public License agreement ("AGPL"), except where a separate commercial license 
 * agreement for PBS Pro version 14 or later has been executed in writing with Altair.
 *  
 * Altair’s dual-license business model allows companies, individuals, and 
 * organizations to create proprietary derivative works of PBS Pro and distribute 
 * them - whether embedded or bundled with other software - under a commercial 
 * license agreement.
 * 
 * Use of Altair’s trademarks, including but not limited to "PBS™", 
 * "PBS Professional®", and "PBS Pro™" and Altair’s logos is subject to Altair's 
 * trademark licensing policies.
 *
 */

#include <stdio.h>
#include <stdlib.h>

#include "pbs_bitmap.h"

#define BYTES_TO_BITS(x) ((x) * 8)



pbs_bitmap *
pbs_bitmap_alloc(pbs_bitmap *pbm, long num_bits)
{
	pbs_bitmap *bm;
	unsigned long *tmp_bits;
	long prev_longs;
	
	if(num_bits <= 0)
		return NULL;
	
	if(pbm == NULL) {
		bm = malloc(sizeof(pbs_bitmap));
		if(bm == NULL)
			return NULL;
		bm->bits = NULL;
		bm->num_longs = 0;
	}
	else 
		bm = pbm;
	
	/* If we have enough unused bits available, we don't need to allocate */
	if (bm->num_longs * BYTES_TO_BITS(sizeof(unsigned long)) >= num_bits) {
		bm->num_bits = num_bits;
		return bm;
	}
		
		
	prev_longs = bm->num_longs;
	
	bm->num_bits = num_bits;
	bm->num_longs = num_bits / BYTES_TO_BITS(sizeof(unsigned long));
	if (num_bits % BYTES_TO_BITS(sizeof(unsigned long)) >= 0)
		bm->num_longs++;
	tmp_bits = calloc(bm->num_longs, sizeof(unsigned long));
	if (tmp_bits == NULL) {
		if(pbm == NULL) /* we allocated the memory */
			pbs_bitmap_free(bm);
		return NULL;
	}
	
	if(bm->bits != NULL) {
		int i;
		for(i = 0; i < prev_longs; i++)
			tmp_bits[i] = bm->bits[i];
		
		free(bm->bits);
	}
	bm->bits = tmp_bits;
		
	return bm;
}

void
pbs_bitmap_free(pbs_bitmap *bm)
{
	if(bm == NULL)
		return;
	free(bm->bits);
	free(bm);
}

int
pbs_bitmap_bit_on(pbs_bitmap *pbm, long bit)
{
	long n;
	unsigned long b;
	unsigned long f;
	
	if(pbm == NULL)
		return 0;
	
	if (bit >= pbm->num_bits) {
		if(pbs_bitmap_alloc(pbm, bit+1) == NULL)
			return 0;
	}
	
	n = bit / BYTES_TO_BITS(sizeof(unsigned long));
	f = bit % BYTES_TO_BITS(sizeof(unsigned long));
	b = 1UL << f;
	
	pbm->bits[n] |= b;
	return 1;
}

int
pbs_bitmap_bit_off(pbs_bitmap *pbm, long bit)
{
	long n;
	unsigned long b;
	
	if (pbm == NULL)
		return 0;
	
	if (bit > pbm->num_bits) {
		if(pbs_bitmap_alloc(pbm, bit+1) == NULL)
			return 0;
	}
	
	n = bit / BYTES_TO_BITS(sizeof(unsigned long));
	b = 1UL << (bit % BYTES_TO_BITS(sizeof(unsigned long)));
	
	pbm->bits[n] &= ~b;
	return 1;
}

int
pbs_bitmap_get_bit(pbs_bitmap *pbm, unsigned long bit)
{
	long n;
	unsigned long b;
	
	if (pbm == NULL)
		return 0;
	
	if (bit > pbm->num_bits)
		return 0;
	
	n = bit / BYTES_TO_BITS(sizeof(unsigned long));
	b = 1UL << (bit % BYTES_TO_BITS(sizeof(unsigned long)));
	
	return (pbm->bits[n] & b) ? 1 : 0;
}

int
pbs_bitmap_equals(pbs_bitmap *L, pbs_bitmap *R)
{
	int i;
	
	if (L == NULL || R == NULL)
		return 0;
	
	/* In the case where R is longer than L, we need to allocate more space for L
	 * Instead of using R->num_bits, we call pbs_bitmap_alloc() with the 
	 * full number of bits required for its num_longs.  This is because it
	 * is possible that R has more space allocated to it than required for its num_bits.
	 * This happens if it had a previous call to pbs_bitmap_equals() with a shorter bitmap.
	 */
	if (R->num_longs > L->num_longs)
		if(pbs_bitmap_alloc(L, BYTES_TO_BITS(R->num_longs * sizeof(unsigned long))) == NULL)
			return 0;
	
	for(i = 0; i < R->num_longs; i++)
		L->bits[i] = R->bits[i];
	if (R->num_longs < L->num_longs)
		for(; i < L->num_longs; i++)
			L->bits[i] = 0;
	
	L->num_bits = R->num_bits;
	return 1;
}

int
pbs_bitmap_is_equal(pbs_bitmap *L, pbs_bitmap *R)
{
	int i;
	
	if(L == NULL || R == NULL)
		return 0;
	
	if (L->num_bits != R->num_bits)
		return 0;
	
	for(i = 0; i < L->num_longs; i++)
		if(L->bits[i] != R->bits[i])
			return 0;
	
	return 1;
}

int
pbs_bitmap_count(pbs_bitmap *bm)
{
	int i;
	int ct = 0;
	for(i = 0; i < bm->num_bits; i++)
		if(pbs_bitmap_get_bit(bm, i))
			ct++;
	
	return ct;
}

#include <string.h>
char *
pbs_bitmap_print(pbs_bitmap *bm) 
{
	int i;
	
	static char buf[2048];
	buf[0] = '\0';
	
	for(i=bm->num_bits; i >= 0; i--)
		strcat(buf, pbs_bitmap_get_bit(bm, i) ? "1" : "0");
	
	return buf;
}

int pbs_bitmap_first_bit(pbs_bitmap *bm)
{
	int i;
	for(i = 0; i < bm->num_bits; i++)
		if(pbs_bitmap_get_bit(bm, i))
			return i;
}