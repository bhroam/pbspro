# coding: utf-8

# Copyright (C) 1994-2016 Altair Engineering, Inc.
# For more information, contact Altair at www.altair.com.
#
# This file is part of the PBS Professional ("PBS Pro") software.
#
# Open Source License Information:
#
# PBS Pro is free software. You can redistribute it and/or modify it under the
# terms of the GNU Affero General Public License as published by the Free
# Software Foundation, either version 3 of the License, or (at your option) any
# later version.
#
# PBS Pro is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
# A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
# details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
# Commercial License Information:
#
# The PBS Pro software is licensed under the terms of the GNU Affero General
# Public License agreement ("AGPL"), except where a separate commercial license
# agreement for PBS Pro version 14 or later has been executed in writing with
# Altair.
#
# Altair’s dual-license business model allows companies, individuals, and
# organizations to create proprietary derivative works of PBS Pro and
# distribute them - whether embedded or bundled with other software - under
# a commercial license agreement.
#
# Use of Altair’s trademarks, including but not limited to "PBS™",
# "PBS Professional®", and "PBS Pro™" and Altair’s logos is subject to Altair's
# trademark licensing policies.

from tests.functional import *


class TestNodeBuckets(TestFunctional):
    """
    Test basic functionality of node buckets.
    """

    def setUp(self):
        TestFunctional.setUp(self)
        self.colors = \
            ['red', 'orange', 'yellow', 'green', 'blue', 'indigo', 'violet']

        self.server.manager(MGR_CMD_CREATE, RSC,
                            {'type': 'string', 'flag': 'h'}, id='color')
        a = {'resources_available.ncpus': 1, 'resources_available.mem': '8gb'}
        # 10010 nodes since it divides into 7 evenly.
        # Each node bucket will have 1430 nodes in it
        self.server.create_vnodes('vnode', a, 10010, self.mom,
                                  sharednode=False,
                                  attrfunc=self.cust_attr_func)
        self.scheduler.add_resource('color')

    def cust_attr_func(self, name, totalnodes, numnode, attribs):
        """
        Add custom resources to nodes
        """
        a = {'resources_available.color': self.colors[numnode % 7]}

        return dict(attribs.items() + a.items())

    def test_basic(self):
        """
        Request nodes of a specific color and make sure they are correctly
        allocated to the job
        """

        a = {'Resource_List.select': '4:ncpus=1:color=yellow',
             'Resource_List.place': 'scatter:excl'}
        J = Job(TEST_USER, a)
        jid = self.server.submit(J)
        self.server.expect(JOB, {'job_state': 'R'}, id=jid)

        js = self.server.status(JOB, id=jid)
        nodes = J.get_vnodes(js[0]['exec_vnode'])
        for node in nodes:
            n = self.server.status(NODE, id=node)
            self.assertTrue('yellow' in n[0]['resources_available.color'])

    def test_multi_bucket(self):
        """
        Request two different chunk types which need to be allocated from
        different buckets and make sure they are allocated correctly.
        """
        a = {'Resource_List.select':
             '4:ncpus=1:color=yellow+4:ncpus=1:color=blue',
             'Resource_List.place': 'scatter:excl'}
        J = Job(TEST_USER, a)
        jid = self.server.submit(J)
        self.server.expect(JOB, {'job_state': 'R'}, id=jid)

        js = self.server.status(JOB, id=jid)
        nodes = J.get_vnodes(js[0]['exec_vnode'])
        # Yellow nodes were requested first.
        # Make sure they come before the blue nodes.
        for i in range(4):
            n = self.server.status(NODE, id=nodes[i])
            self.assertTrue('yellow' in n[0]['resources_available.color'])
        for i in range(4, 8):
            n = self.server.status(NODE, id=nodes[i])
            self.assertTrue('blue' in n[0]['resources_available.color'])

    def test_multi_bucket2(self):
        """
        Request nodes from all 7 different buckets and see them allocated
        correctly
        """
        select = ""
        for c in self.colors:
            select += "1:ncpus=1:color=%s+" % (c)

        select = select[:-1]

        a = {'Resource_List.select': select,
             'Resource_List.place': 'scatter:excl'}

        J = Job(TEST_USER, a)
        jid = self.server.submit(J)
        self.server.expect(JOB, {'job_state': 'R'}, id=jid)

        js = self.server.status(JOB, id=jid)
        nodes = J.get_vnodes(js[0]['exec_vnode'])
        for i in range(len(nodes)):
            n = self.server.status(NODE, id=nodes[i])
            self.assertTrue(self.colors[i] in 
                            n[0][ 'resources_available.color'])

    def test_not_run(self):
        """
        Request more nodes of one color that is available to make sure
        the job is not run on incorrect nodes.
        """
        a = {'Resource_List.select': '1431:ncpus=1:color=yellow',
             'Resource_List.place': 'scatter:excl'}
        J = Job(TEST_USER, a)
        jid = self.server.submit(J)
        self.server.expect(JOB, 'comment', op=SET)
        self.server.expect(JOB, {'job_state': 'Q'}, id=jid)
        sj = self.server.status(JOB, id=jid)
        self.assertTrue(sj[0]['comment'].startswith("Can Never Run"))
