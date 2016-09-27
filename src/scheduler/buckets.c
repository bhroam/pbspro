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
#include <string.h>
#include <errno.h>
#include "data_types.h"
#include "pbs_bitmap.h"
#include "node_info.h"
#include "server_info.h"
#include "buckets.h"
#include "globals.h"
#include "resource.h"
#include "resource_resv.h"
#include "simulate.h"
#include "misc.h"

bucket_bitpool *
new_bucket_bitpool() {
	bucket_bitpool *bp;
	
	bp = malloc(sizeof(bucket_bitpool));
	if(bp == NULL) {
		log_err(errno, __func__, MEM_ERR_MSG);
		return NULL;
	}
	
	bp->checkpoint = pbs_bitmap_alloc(NULL, 1);
	if(bp->checkpoint == NULL) {
		free_bucket_bitpool(bp);
		return NULL;
	}
	bp->checkpoint_ct = 0;
	
	bp->truth = pbs_bitmap_alloc(NULL, 1);
	if(bp->truth == NULL) {
		free_bucket_bitpool(bp);
		return NULL;
	}
	bp->truth_ct = 0;
	
	bp->working = pbs_bitmap_alloc(NULL, 1);
	if(bp->working == NULL) {
		free_bucket_bitpool(bp);
		return NULL;
	}
	bp->working_ct = 0;
	
	return bp;
}

void
free_bucket_bitpool(bucket_bitpool *bp) {
	if(bp == NULL)
		return;
	
	pbs_bitmap_free(bp->checkpoint);
	pbs_bitmap_free(bp->truth);
	pbs_bitmap_free(bp->working);
	
	free(bp);
}

bucket_bitpool *
dup_bucket_bitpool(bucket_bitpool *obp) {
	bucket_bitpool *nbp;
	
	nbp = new_bucket_bitpool();
	if(pbs_bitmap_equals(nbp->checkpoint, obp->checkpoint) == 0) {
		free_bucket_bitpool(nbp);
		return NULL;
	}
	nbp->checkpoint_ct = obp->checkpoint_ct;
	
	if(pbs_bitmap_equals(nbp->truth, obp->truth) == 0) {
		free_bucket_bitpool(nbp);
		return NULL;
	}
	nbp->truth_ct = obp->truth_ct;
	
	if(pbs_bitmap_equals(nbp->working, obp->working) == 0) {
		free_bucket_bitpool(nbp);
		return NULL;
	}
	nbp->working_ct = obp->working_ct;
	
	return nbp;
}

node_bucket *
new_node_bucket(int new_pools) {
	node_bucket *nb;
	
	nb = malloc(sizeof(node_bucket));
	if(nb == NULL) {
		log_err(errno, __func__, MEM_ERR_MSG);
		return NULL;
	}

	if (new_pools) {
		nb->busy = new_bucket_bitpool();
		if (nb->busy == NULL) {
			free_node_bucket(nb);
			return NULL;
		}

		nb->busy_later = new_bucket_bitpool();
		if (nb->busy_later == NULL) {
			free_node_bucket(nb);
			return NULL;
		}

		nb->free = new_bucket_bitpool();
		if (nb->free == NULL) {
			free_node_bucket(nb);
			return NULL;
		}
	}
	else {
		nb->busy = NULL;
		nb->busy_later = NULL;
		nb->free = NULL;
	}
	nb->bkt_nodes = pbs_bitmap_alloc(NULL, 1);
	if(nb->bkt_nodes == NULL) {
		free_node_bucket(nb);
		return NULL;
	}
		
	nb->res_spec = NULL;
	nb->queue = NULL;
	nb->total = 0;
	nb->down_offline = 0;
	
	return nb;
}

node_bucket *
dup_node_bucket(node_bucket *onb, server_info *nsinfo) {
	node_bucket *nnb;
	
	nnb = new_node_bucket(0);
	if(nnb == NULL)
		return NULL;
	
	nnb->busy = dup_bucket_bitpool(onb->busy);
	if(nnb->busy == NULL) {
		free_node_bucket(nnb);
		return NULL;
	}
	
	nnb->busy_later = dup_bucket_bitpool(onb->busy_later);
	if(nnb->busy_later == NULL) {
		free_node_bucket(nnb);
		return NULL;
	}
	
	nnb->free = dup_bucket_bitpool(onb->free);
	if(nnb->free == NULL) {
		free_node_bucket(nnb);
		return NULL;
	}
	
	pbs_bitmap_equals(nnb->bkt_nodes, onb->bkt_nodes);
	nnb->res_spec = dup_resource_list(onb->res_spec);
	if(nnb->res_spec == NULL) {
		free_node_bucket(nnb);
		return NULL;
	}
	
	if (onb->queue != NULL) {
		nnb->queue = find_queue_info(nsinfo->queues, onb->queue->name);
	}
	
	nnb->total = onb->total;
	nnb->down_offline = onb->down_offline;
	
	return nnb;
}
node_bucket **
dup_node_bucket_array(node_bucket **old, server_info *nsinfo) {
	node_bucket **new;
	int i;
	if(old == NULL)
		return NULL;
	
	new = malloc((count_array((void**) old)+1) * sizeof(node_bucket *));
	if(new == NULL) {
		log_err(errno, __func__, MEM_ERR_MSG);
		return NULL;
	}
	
	for(i = 0; old[i] != NULL; i++) {
		new[i] = dup_node_bucket(old[i], nsinfo);
		if(new[i] == NULL) {
			free_node_bucket_array(new);
			return NULL;
		}
	}
	
	new[i] = NULL;
	
	return new;
}
void
free_node_bucket(node_bucket *nb) {
	if(nb == NULL)
		return;
	
	free_bucket_bitpool(nb->busy);
	free_bucket_bitpool(nb->busy_later);
	free_bucket_bitpool(nb->free);
	
	free_resource_list(nb->res_spec);
	
	pbs_bitmap_free(nb->bkt_nodes);
	free(nb);
}

void
free_node_bucket_array(node_bucket **buckets) {
	int i;
	
	if(buckets == NULL)
		return;
	
	for(i = 0; buckets[i] != NULL; i++)
		free_node_bucket(buckets[i]);
	
	free(buckets);
}


int
find_node_bucket_ind(node_bucket **buckets, schd_resource *rl, queue_info *qinfo) {
	int i;
	if(buckets == NULL || rl == NULL)
		return -1;
	
	for(i = 0; buckets[i] != NULL; i++) {
		if(compare_resource_avail_list(buckets[i]->res_spec, rl)) {
			if(buckets[i]->queue == qinfo)
				return i;
		}
	}
	return -1;
}

node_bucket **
create_node_buckets(server_info *sinfo) {
	int i;
	int j = 0;
	node_bucket **buckets = NULL;
	node_bucket **tmp;
	snode **snodes;
	int snode_ct;
	
	if(sinfo == NULL)
		return NULL;
	
	snodes = sinfo->snodes;
	snode_ct = count_array((void**) snodes);
	
	buckets = calloc((snode_ct + 1), sizeof(node_bucket *));
	if(buckets == NULL) {
		log_err(errno, __func__, MEM_ERR_MSG);
		return NULL;
	}
		
	
	for(i = 0; snodes[i] != NULL; i++) {
		node_bucket *nb = NULL;
		int ind;
		queue_info *qinfo = NULL;

		if(snodes[i]->ninfo->queue_name != NULL)
			qinfo = find_queue_info(sinfo->queues, snodes[i]->ninfo->queue_name);
		
		ind = find_node_bucket_ind(buckets, snodes[i]->ninfo->res, qinfo);
		if(ind == -1)
			snodes[i]->bucket_ind = j;
		else {
			snodes[i]->bucket_ind = ind;
			nb = buckets[ind];
		}
		
		if(nb != NULL) {
			
			pbs_bitmap_bit_on(nb->bkt_nodes, i);
			nb->total++;
			if(snodes[i]->ninfo->is_free) {
				if(snodes[i]->node_events != NULL) {
					pbs_bitmap_bit_on(nb->busy_later->truth, i);
					nb->busy_later->truth_ct++;
				}
				else {
					pbs_bitmap_bit_on(nb->free->truth, i);
					nb->free->truth_ct++;
				}
			} else {
				pbs_bitmap_bit_on(nb->busy->truth, i);
				nb->busy->truth_ct++;
			}
			if(snodes[i]->ninfo->is_down || snodes[i]->ninfo->is_offline)
				nb->down_offline++;
		} else { /* no bucket found, need to add one*/
			schd_resource *cur_res;
			buckets[j] = new_node_bucket(1);
			
			if(buckets[j] == NULL) {
				free_node_bucket_array(buckets);
				return NULL;
			}
			
			if(conf.node_bucket_resdef != NULL)
				buckets[j]->res_spec = dup_selective_resource_list(snodes[i]->ninfo->res, conf.node_bucket_resdef, NO_FLAGS);
			else
				buckets[j]->res_spec = dup_resource_list(snodes[i]->ninfo->res);
			
			if(buckets[j]->res_spec == NULL) {
				free_node_bucket_array(buckets);
				return NULL;
			}
			
			if(snodes[i]->ninfo->queue_name != NULL)
				buckets[j]->queue = qinfo;
			
			for(cur_res = buckets[j]->res_spec; cur_res != NULL; cur_res = cur_res->next) 
				if(cur_res->type.is_consumable)
					cur_res->assigned = 0;

			
			pbs_bitmap_bit_on(buckets[j]->bkt_nodes, i);
			if(snodes[i]->ninfo->is_free) {
				if(snodes[i]->node_events != NULL) {
					pbs_bitmap_bit_on(buckets[j]->busy_later->truth, i);
					buckets[j]->busy_later->truth_ct = 1;
				}
				else {
					pbs_bitmap_bit_on(buckets[j]->free->truth, i);
					buckets[j]->free->truth_ct = 1;
				}
			} else {
				pbs_bitmap_bit_on(buckets[j]->busy->truth, i);
				buckets[j]->busy->truth_ct = 1;
			}
			buckets[j]->total = 1;
			if(snodes[i]->ninfo->is_down || snodes[i]->ninfo->is_offline)
				buckets[j]->down_offline = 1;
			j++;
		}
	}
	
	tmp = realloc(buckets, (j+1) * sizeof(node_bucket *));
	if(tmp != NULL)
		buckets = tmp;
	else {
		log_err(errno, __func__, MEM_ERR_MSG);
		return NULL;

	}
	return buckets;
}

chunk_map *
new_chunk_map() {
	chunk_map *cmap;
	cmap = malloc(sizeof(chunk_map));
	if(cmap == NULL) {
		log_err(errno, __func__, MEM_ERR_MSG);
		return NULL;
	}
	
	cmap->chunk = NULL;
	cmap->bkts = NULL;
	cmap->node_bits = pbs_bitmap_alloc(NULL, 1);
	if(cmap->node_bits == NULL) {
		free_chunk_map(cmap);
	}
	
	return cmap;
}

chunk_map *
dup_chunk_map(chunk_map *ocmap) {
	chunk_map *ncmap;
	int i;
	
	ncmap = new_chunk_map();
	if(ncmap == NULL)
		return NULL;
	
	ncmap->chunk = ocmap->chunk;
	ncmap->bkts = calloc(count_array((void **)ocmap->bkts)+1, sizeof(node_bucket *));
	if(ncmap->bkts == NULL) {
		free(ncmap);
		return NULL;
	}
	for(i = 0; ocmap->bkts[i] != NULL; i++)
		ncmap->bkts[i] = ocmap->bkts[i];
	
	ncmap->bkts[i] = NULL;
	
	pbs_bitmap_equals(ncmap->node_bits, ocmap->node_bits);
	
	return ncmap;
}

void 
free_chunk_map(chunk_map *cmap) {
	if(cmap == NULL)
		return;
	
	free(cmap->bkts);
	pbs_bitmap_free(cmap->node_bits);
	free(cmap);
}

void 
free_chunk_map_array(chunk_map **cmap_arr) {
	int i;
	if(cmap_arr == NULL)
		return;
	
	for(i = 0; cmap_arr[i] != NULL; i++)
		free_chunk_map(cmap_arr[i]);
	
	free(cmap_arr);
}

chunk_map **
dup_chunk_map_array(chunk_map **ocmap_arr) {
	chunk_map **ncmap_arr;
	int i;
	
	if(ocmap_arr == NULL)
		return NULL;
	
	ncmap_arr = malloc((count_array((void**) ocmap_arr)) * sizeof(chunk_map));
	if(ncmap_arr == NULL) {
		log_err(errno, __func__, MEM_ERR_MSG);
		return NULL;
	}
	
	for(i = 0; ocmap_arr[i] != NULL; i++) {
		ncmap_arr[i] = dup_chunk_map(ocmap_arr[i]);
		if(ncmap_arr[i] == NULL) {
			free_chunk_map_array(ncmap_arr);
			return NULL;
		}
	}
	ncmap_arr[i] = NULL;
	
	return ncmap_arr;
}

void
set_working_bucket_to_truth(node_bucket *nb) {
	if(nb == NULL)
		return;

	pbs_bitmap_equals(nb->busy->working, nb->busy->truth);
	nb->busy->working_ct = nb->busy->truth_ct;
	
	pbs_bitmap_equals(nb->busy_later->working, nb->busy_later->truth);
	nb->busy_later->working_ct = nb->busy_later->truth_ct;
	
	pbs_bitmap_equals(nb->free->working, nb->free->truth);
	nb->free->working_ct = nb->free->truth_ct;
}

void
set_chkpt_bucket_to_working(node_bucket *nb) {
	if(nb == NULL)
		return;

	pbs_bitmap_equals(nb->busy->checkpoint, nb->busy->working);
	nb->busy->checkpoint_ct = nb->busy->working_ct;
	
	pbs_bitmap_equals(nb->busy_later->checkpoint, nb->busy_later->working);
	nb->busy_later->checkpoint_ct = nb->busy_later->working_ct;
	
	pbs_bitmap_equals(nb->free->checkpoint, nb->free->working);
	nb->free->checkpoint_ct = nb->free->working_ct;
}

void
set_working_bucket_to_chkpt(node_bucket *nb) {
	if(nb == NULL)
		return;

	pbs_bitmap_equals(nb->busy->working, nb->busy->checkpoint);
	nb->busy->working_ct = nb->busy->checkpoint_ct;
	
	pbs_bitmap_equals(nb->busy_later->working, nb->busy_later->checkpoint);
	nb->busy_later->working_ct = nb->busy_later->checkpoint_ct;
	
	pbs_bitmap_equals(nb->free->working, nb->free->checkpoint);
	nb->free->working_ct = nb->free->checkpoint_ct;
}
void
set_chkpt_bucket_to_truth(node_bucket *nb) {
	if(nb == NULL)
		return;

	pbs_bitmap_equals(nb->busy->checkpoint, nb->busy->truth);
	nb->busy->checkpoint_ct = nb->busy->truth_ct;
	
	pbs_bitmap_equals(nb->busy_later->checkpoint, nb->busy_later->truth);
	nb->busy_later->checkpoint_ct = nb->busy_later->truth_ct;
	
	pbs_bitmap_equals(nb->free->checkpoint, nb->free->truth);
	nb->free->checkpoint_ct = nb->free->truth_ct;
}


void
set_truth_bucket_to_chkpt(node_bucket *nb) {
	if(nb == NULL)
		return;

	pbs_bitmap_equals(nb->busy->truth, nb->busy->checkpoint);
	nb->busy->truth_ct = nb->busy->checkpoint_ct;
	
	pbs_bitmap_equals(nb->busy_later->truth, nb->busy_later->checkpoint);
	nb->busy_later->truth_ct = nb->busy_later->checkpoint_ct;
	
	pbs_bitmap_equals(nb->free->truth, nb->free->checkpoint);
	nb->free->truth_ct = nb->free->checkpoint_ct;
}

int
bucket_match(chunk_map **cmap, resource_resv *resresv, time_t time_now)
{
	int i;
	int j;
	int k;
	int num_chunks_needed = 0;
	static pbs_bitmap *zeromap = NULL;
	
	if (cmap == NULL || resresv == NULL || resresv->select == NULL)
		return 0;

	if (zeromap == NULL) {
		zeromap = pbs_bitmap_alloc(NULL, 1);
		if (zeromap == NULL)
			return 0;
	}

	for (i = 0; cmap[i] != NULL; i++) {
		for (j = 0; cmap[i]->bkts[j] != NULL; j++) {
			set_working_bucket_to_truth(cmap[i]->bkts[j]);
			set_chkpt_bucket_to_working(cmap[i]->bkts[j]);
			pbs_bitmap_equals(cmap[i]->node_bits, zeromap);
		}
	}

	for (i = 0; cmap[i] != NULL; i++) {
		num_chunks_needed = cmap[i]->chunk->num_chunks;
		;

		for (j = 0; cmap[i]->bkts[j] != NULL && num_chunks_needed > 0; j++) {
			node_bucket *bkt = cmap[i]->bkts[j];
			int chunks_added = 0;

			set_working_bucket_to_chkpt(bkt);

			for (k = 0; num_chunks_needed > chunks_added && bkt->busy_later->working_ct > 0 &&
			     k < bkt->busy_later->working->num_bits; k++) {
				if (pbs_bitmap_get_bit(bkt->busy_later->working, k)) {
					if (node_can_fit_job_time(k, resresv, time_now)) {
						pbs_bitmap_bit_off(bkt->busy_later->working, k);
						bkt->busy_later->working_ct--;
						pbs_bitmap_bit_on(bkt->busy->working, k);
						bkt->busy->working_ct++;
						pbs_bitmap_bit_on(cmap[i]->node_bits, k);
						chunks_added++;
					}
				}
			}

			for (k = 0; num_chunks_needed > chunks_added && bkt->free->working_ct > 0 &&
			     k < bkt->free->working->num_bits; k++) {
				if (pbs_bitmap_get_bit(bkt->free->working, k)) {
					pbs_bitmap_bit_off(bkt->free->working, k);
					bkt->free->working_ct--;
					pbs_bitmap_bit_on(bkt->busy->working, k);
					bkt->busy->working_ct++;
					pbs_bitmap_bit_on(cmap[i]->node_bits, k);
					chunks_added++;
				}
			}

			if (chunks_added > 0) {
				num_chunks_needed -= chunks_added;
				set_chkpt_bucket_to_working(bkt);
			}
		}
		/* Couldn't find buckets to satisfy all the chunks */
		if (num_chunks_needed > 0)
			return 0;
	}
	
	return 1;
}

int
node_can_fit_job_time(int node_ind, resource_resv *resresv, time_t time_now)
{
	te_list *tel;
	time_t end;
	resource_req *req;
	server_info *sinfo;
	
	if(resresv == NULL)
		return 0;

	sinfo = resresv->server;
	req = find_resource_req(resresv->resreq, getallres(RES_WALLTIME));
	if (req != NULL)
		end = time_now + req->amount;
	else
		end = time_now + FIVE_YRS;

	tel = sinfo->snodes[node_ind]->node_events;
	if(tel != NULL)
		if (tel->event != NULL)
			if (tel->event->event_time < end)
				if (tel->event->event_type == TIMED_RUN_EVENT)
					return 0;
	
	return 1;
}

nspec *
chunk_to_nspec(status *policy, chunk *chk, node_info *node)
{
	nspec *ns;
	resource_req *prev_req;
	resource_req *req;
	resource_req *cur_req;
	
	if(chk == NULL)
		return NULL;
	
	ns = new_nspec();
	if (ns == NULL)
		return NULL;

	ns->ninfo = node;
	ns->seq_num = get_sched_rank();
	ns->end_of_chunk = 1;
	prev_req = NULL;
	for (cur_req = chk->req; cur_req != NULL; cur_req = cur_req->next) {
		if (resdef_exists_in_array(policy->resdef_to_check, cur_req->def) && cur_req->def->type.is_consumable) {
			req = dup_resource_req(cur_req);
			if (req == NULL) {
				free_nspec(ns);
				return NULL;
			}
			if (prev_req == NULL)
				ns->resreq = req;
			else
				prev_req->next = req;
			prev_req = req;
		}
	}
	
	return ns;
}

nspec **
bucket_to_nspecs(status *policy, chunk_map **cb_map, resource_resv *resresv)
{
	int i;
	int j;
	int k;
	int n = 0;
	nspec **ns_arr;
	int chunks_needed;
	server_info *sinfo;
	
	if(policy == NULL || cb_map == NULL || resresv == NULL)
		return NULL;
		
	sinfo = resresv->server;
	ns_arr = calloc(resresv->select->total_chunks + 1, sizeof(nspec*));
	if(ns_arr == NULL) {
		log_err(errno, __func__, MEM_ERR_MSG);
		return NULL;
	}
		
	for(i = 0; cb_map[i] != NULL; i++) {
		for(j = 0; j < cb_map[i]->node_bits->num_bits; j++) {
			if(pbs_bitmap_get_bit(cb_map[i]->node_bits, j)) {
				ns_arr[n] = chunk_to_nspec(policy, cb_map[i]->chunk, sinfo->snodes[j]->ninfo);
				if(ns_arr[n] == NULL) {
					/* NULL terminate the array so we can free it */
					ns_arr[n+1] = NULL;
					free_nspecs(ns_arr);
					return NULL;
				}
				n++;
			}
		}
	}
	ns_arr[n] = NULL;

	return ns_arr;
}

snode *
new_snode()
{
	snode *s;
	
	s = malloc(sizeof(snode));
	if(s == NULL) {
		log_err(errno, __func__, MEM_ERR_MSG);
		return NULL;
	}
	
	s->bucket_ind = -1;
	s->ninfo = NULL;
	s->node_events = NULL;
	
	return s;
}

void
free_snode(snode *s)
{
	if(s == NULL)
		return;
	free_te_list(s->node_events);
	free(s);
}

void
free_snode_array(snode **sa)
{
	int i;
	if(sa == NULL)
		return;
	
	for(i = 0; sa[i] != NULL; i++)
		free_snode(sa[i]);
	
	free(sa);
}

snode *
copy_snode(snode *osn)
{
	snode *nsn;
	
	nsn = new_snode();
	if(nsn == NULL)
		return NULL;
	
	nsn->bucket_ind = osn->bucket_ind;
	nsn->ninfo = osn->ninfo;
	nsn->node_events = dup_te_lists(osn->node_events, NULL, 1);
	
	return nsn;
}

snode **
copy_snode_array(snode **osa)
{
	int i;
	int cnt;
	snode **nsa;
	
	cnt = count_array((void**) osa);
	
	nsa = malloc((cnt+1) * sizeof(snode *));
	
	if(nsa == NULL) {
		log_err(errno, __func__, MEM_ERR_MSG);
		return NULL;
	}
	
	for(i = 0; i < cnt; i++) {
		nsa[i] = copy_snode(osa[i]);
	}
	nsa[i] = NULL;
	
	return nsa;
}

int
find_snode_ind(snode **snodes, int rank) {
	int i;
	
	if(snodes == NULL)
		return -1;
	
	for(i = 0; snodes[i] != NULL; i++) {
		if(snodes[i]->ninfo->rank == rank)
			return i;
	}
}

snode **
create_snodes(node_info **nodes)
{
	snode **snodes;
	int i;
	int ct;
	
	ct = count_array((void**) nodes);
	
	snodes = malloc((ct+1) * sizeof(snode *));
	if(snodes == NULL) {
		log_err(errno, __func__, MEM_ERR_MSG);
		return NULL;
	}
	
	
	snodes[0] = NULL;
	for(i = 0; i < ct; i++) {
		snodes[i] = new_snode();
		if(snodes[i] == NULL) {
			free_snode_array(snodes);
			return NULL;
		}
		snodes[i]->ninfo = nodes[i];
		snodes[i]->node_events = dup_te_lists(nodes[i]->node_events, NULL, 1);
	}
	snodes[i] = NULL;
	return snodes;
}

int job_should_use_buckets(resource_resv *resresv) {
	int i;
	
	/* qrun uses the standard path */
	if(resresv == resresv->server->qrun_job)
		return 0;
	
	/* Job's in reservations use the standard path */
	if(resresv->job != NULL) {
		if(resresv->job->resv != NULL)
			return 0;
	}
	
	/* Only excl jobs use buckets */
	if(resresv->place_spec->share)
		return 0;
	
	/* STF jobs use the standard path */
	if(resresv->is_shrink_to_fit)
		return 0;
	
	if(!resresv->place_spec->scatter)
		return 0;
	
	if(!resresv->place_spec->excl && !resresv->place_spec->exclhost)
		return 0;
	
	/* Job's requesting specific hosts or vnodes use the standard path */
	for(i = 0; resresv->select->chunks[i] != NULL; i++) {
		if(find_resource_req(resresv->select->chunks[i]->req, getallres(RES_HOST)) != NULL)
			return 0;
		if(find_resource_req(resresv->select->chunks[i]->req, getallres(RES_VNODE)) != NULL)
			return 0;
	}
	
	return 1;
		
}