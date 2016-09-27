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

#ifdef	__cplusplus
extern "C" {
#endif
#ifndef _BUCKETS_H
#define _BUCKETS_H

bucket_bitpool *new_bucket_bitpool();
void free_bucket_bitpool(bucket_bitpool *bp);
bucket_bitpool *dup_bucket_bitpool(bucket_bitpool *obp);

node_bucket *new_node_bucket(int new_pools);
node_bucket *dup_node_bucket(node_bucket *onb, server_info *nsinfo);
node_bucket **dup_node_bucket_array(node_bucket **old, server_info *nsinfo);
void free_node_bucket(node_bucket *nb);
void free_node_bucket_array(node_bucket **buckets);
int find_node_bucket_ind(node_bucket **buckets, schd_resource *rl, queue_info *queue);
node_bucket **create_node_buckets(server_info *sinfo);

int bucket_match(chunk_map **cmap, resource_resv *resresv, time_t time_now);
nspec *chunk_to_nspec(status *policy, chunk *chk, node_info *node);
nspec **bucket_to_nspecs(status *policy, chunk_map **cmap, resource_resv *resresv);
int node_can_fit_job_time(int node_ind, resource_resv *resresv, time_t time_now);

void set_working_bucket_to_truth(node_bucket *nb);
void set_chkpt_bucket_to_working(node_bucket *nb);
void set_working_bucket_to_chkpt(node_bucket *nb);
void set_chkpt_bucket_to_truth(node_bucket *nb);
void set_truth_bucket_to_chkpt(node_bucket *nb);


chunk_map *new_chunk_map();
chunk_map *dup_chunk_map(chunk_map *ocmap);
void free_chunk_map(chunk_map *cmap);
void free_chunk_map_array(chunk_map **cmap_arr);
chunk_map **dup_chunk_map_array(chunk_map **ocmap_arr);

int find_snode_ind(snode **snodes, int rank);
snode **create_snodes(node_info **nodes);
snode *new_snode();
void free_snode(snode *s);
void free_snode_array(snode **sa);
snode *copy_snode(snode *os);
snode **copy_snode_array(snode **osa);

int job_should_use_buckets(resource_resv *resresv);









#ifdef	__cplusplus
}
#endif
#endif	/* _BUCKETS_H */