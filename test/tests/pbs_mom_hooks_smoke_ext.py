# coding: utf-8

# Copyright (C) 2003-2021 Altair Engineering, Inc. All rights reserved.
# Copyright notice does not imply publication.
#
# ALTAIR ENGINEERING INC. Proprietary and Confidential. Contains Trade Secret
# Information. Not for use or disclosure outside of Licensee's organization.
# The software and information contained herein may only be used internally and
# is provided on a non-exclusive, non-transferable basis. License may not
# sublicense, sell, lend, assign, rent, distribute, publicly display or
# publicly perform the software or other information provided herein,
# nor is Licensee permitted to decompile, reverse engineer, or
# disassemble the software. Usage of the software and other information
# provided by Altair(or its resellers) is only as explicitly stated in the
# applicable end user license agreement between Altair and Licensee.
# In the absence of such agreement, the Altair standard end user
# license agreement terms shall govern.
from tests.functional import *


@tags('hooks', 'smoke')
class TestMomHooksSmoke_ext(TestFunctional):
    """
    This test suite contains smoke test cases for
    mom hooks feature
    """

    testhook = """import pbs
job = pbs.event().job
job.resources_used['fooS'] = 10
pbs.logmsg(pbs.LOG_DEBUG, "%s" % (str(job.resources_used)))
"""

    def setUp(self):
        TestFunctional.setUp(self)
        # Retrieve pbs.conf location
        self.pbs_conf_path = self.du.get_pbs_conf_file(hostname=self.
                                                       server.hostname)
        msg = "Unable to retrieve pbs.conf file path"
        self.assertNotEqual(self.pbs_conf_path, None, msg)
        self.logger.info("pbs.conf file path retrieved successfully")

    def test_t16(self):
        """
        Periodic hook should be accepted on MoMs where /etc/pbs.conf file
        is moved using PBS_CONF_FILE variable
        """
        rc = self.mom.add_config({'$logevent': '0xffffffff'})
        msg = "Could not update mom_config file,error: %s" % rc['err']
        self.assertEqual(rc['rc'], 0, msg)
        self.logger.info("mom config file has been updated successfully")

        # Create and import hook contents
        hook_name = "test"
        hook_body = "import pbs\ne = pbs.event()"
        hook_body += "\nif e.type == pbs.EXECHOST_PERIODIC:"
        hook_body += '\n\tpbs.logmsg(pbs.LOG_DEBUG, "event is %s"'
        hook_body += '\n\t% ("EXECHOST_PERIODIC"))'
        a = {'event': 'exechost_periodic', 'enabled': 'True'}
        rv = self.server.create_import_hook(hook_name, a, hook_body)
        msg = "Error while creating and importing hook contents"
        self.assertTrue(rv, msg)
        msg = "Hook %s created and " % hook_name
        msg += "hook script is imported successfully"
        self.logger.info(msg)

        # Stop PBS services
        self.server.stop()
        self.logger.info("PBS services stopped successfully")

        # Retrieve temporary directory
        tmp_dir = self.du.get_tempdir(hostname=self.server.hostname)
        msg = "Unable to get temp_dir"
        self.assertNotEqual(tmp_dir, None, msg)
        self.new_conf_path = os.path.join(tmp_dir, "pbs.conf")

        # Copy pbs.conf file to temporary location
        cmd = "cp %s %s" % (self.pbs_conf_path, self.new_conf_path)
        rc = self.du.run_cmd(hosts=self.server.hostname, cmd=cmd)
        msg = "Cannot copy %s " % self.pbs_conf_path
        msg += "%s, error: %s" % (self.new_conf_path, rc['err'])
        self.assertEqual(rc['rc'], 0, msg)

        # Set the PBS_CONF_FILE variable to the temp location
        os.environ['PBS_CONF_FILE'] = self.new_conf_path
        self.logger.info("Successfully exported PBS_CONF_FILE variable")

        self.server.pi.conf_file = self.new_conf_path
        self.server.start()
        self.logger.info("PBS services started successfully")

        # Check mom logs for existence of log message
        log_msg = "Hook;%s;periodic hook accepted" % hook_name
        self.mom.log_match(log_msg, starttime=self.mom.ctime)
        self.logger.info("Expected msg found in mom-logs")

    @tags('container_runnable')
    @requirements(num_moms=2)
    def test_t11(self):
        """
        Mom hook event 'execjob_prologue' should be able to modify job/node
        attributes
        """
        # Skip test if number of mom provided is not equal to two
        if len(self.moms) != 2:
            self.skipTest("test requires two MoMs as input, " +
                          "use -p moms=<mom1:mom2>")
        self.momA = self.moms.values()[0]
        self.momB = self.moms.values()[1]

        # Get the current time
        sub_time = int(time.time())

        # Create and import hook contents
        a = {'event': 'execjob_prologue', 'enabled': 'True'}
        hook_name = "test"

        hook_body = """import pbs
import os
import sys


def print_attribs(pbs_obj):
   for a in pbs_obj.attributes:
      v = getattr(pbs_obj, a)
      if (v != None) and str(v) != "":
         pbs.logmsg(pbs.LOG_DEBUG, "%s = %s" % (a,v))

e = pbs.event()

pbs.logmsg(pbs.LOG_DEBUG, "printing pbs.event() values -------->")
if e.type == pbs.EXECJOB_BEGIN:
   pbs.logmsg(pbs.LOG_DEBUG, "event is %s" % ("EXECJOB_BEGIN"))
elif e.type == pbs.EXECJOB_PROLOGUE:
   pbs.logmsg(pbs.LOG_DEBUG, "event is %s" % ("EXECJOB_PROLOGUE"))
elif e.type == pbs.EXECJOB_PRETERM:
   pbs.logmsg(pbs.LOG_DEBUG, "event is %s" % ("EXECJOB_PRETERM"))
elif e.type == pbs.EXECJOB_EPILOGUE:
   pbs.logmsg(pbs.LOG_DEBUG, "event is %s" % ("EXECJOB_EPILOGUE"))
elif e.type == pbs.EXECJOB_END:
   pbs.logmsg(pbs.LOG_DEBUG, "event is %s" % ("EXECJOB_END"))
elif e.type == pbs.EXECHOST_PERIODIC:
   pbs.logmsg(pbs.LOG_DEBUG, "event is %s" % ("EXECHOST_PERIODIC"))
else:
   pbs.logmsg(pbs.LOG_DEBUG, "event is %s" % ("UNKNOWN"))


pbs.logmsg(pbs.LOG_DEBUG, "hook_name is %s" % (e.hook_name))
pbs.logmsg(pbs.LOG_DEBUG, "hook_type is %s" % (e.hook_type))
pbs.logmsg(pbs.LOG_DEBUG, "requestor is %s" % (e.requestor))
pbs.logmsg(pbs.LOG_DEBUG, "requestor_host is %s" % (e.requestor_host))

pbs.logmsg(pbs.LOG_DEBUG, "printing pbs.event().job  ----------------->")
print_attribs(e.job)

pbs.logmsg(pbs.LOG_DEBUG, "Executing User=%s\
-------->" % (os.popen('whoami').read().rstrip()))

# Setting job attributes
e.job.Variable_List["BONJOUR"] = "Mounsieur Shlomi"


# Getting/setting vnode_list
vn = pbs.event().vnode_list
local_node = pbs.get_local_nodename()

for k in vn.keys():
   pbs.logmsg(pbs.LOG_DEBUG, "vn[%s]-------------->" % (k))
   print_attribs(vn[k])
   if (local_node in k):
       vn[k].resources_available["file"] = pbs.size("1gb")
"""
        rv = self.server.create_import_hook(hook_name, a, hook_body)
        msg = "Error while creating and importing hook contents"
        self.assertTrue(rv, msg)
        msg = "Hook %s created and " % hook_name
        msg += "hook script is imported successfully"
        self.logger.info(msg)

        # Submit Job and check state of the job to be R
        if self.momA.is_cpuset_mom() or self.momB.is_cpuset_mom():
            node_status = self.server.status(NODE)

        if self.momA.is_cpuset_mom():
            vnodeA = node_status[1]['id']
        else:
            vnodeA = self.momA.shortname
        if self.momB.is_cpuset_mom():
            vnodeB = node_status[-1]['id']
        else:
            vnodeB = self.momB.shortname

        select_stmt = "vnode=%s+" % vnodeA
        select_stmt += "vnode=%s:mem=4mb" % vnodeB
        attrib = {'Resource_List.select': select_stmt}
        j = Job(TEST_USER)
        j.set_attributes(attrib)
        j.create_script("pbsdsh -n 1 /bin/date\nsleep 10")
        jid = self.server.submit(j)
        self.server.expect(JOB, {'job_state': 'R'}, id=jid)

        # Check 'Varibale_List' job attribute
        rc = self.server.status(JOB, attrib='Variable_List', id=jid)
        msg = "Didn't get expected value for Variable 'BONJOUR' in qstat -f"
        self.assertTrue('BONJOUR=Mounsieur Shlomi' in rc[0]['Variable_List'],
                        msg)
        msg = "Got the expected value for Variable 'BONJOUR' in qstat -f"
        self.logger.info(msg)

        # Check 'resources_available.file' attribute on both the nodes
        msg = "Din't get expected value for attribute "
        msg += "'resources_available.file'"
        for node in [vnodeA, vnodeB]:
            node_data = self.server.status(NODE,
                                           attrib='resources_available.file',
                                           id=node)[0]
            self.assertEqual(node_data['resources_available.file'], '1gb', msg)
        msg = "Got expected value for attribute 'resources_available.file'"
        msg += " on both nodes"
        self.logger.info(msg)

        # Check log msgs on both the moms
        log_msg = ["Hook;pbs_python;event is EXECJOB_PROLOGUE",
                   "Hook;pbs_python;hook_name is test",
                   "Hook;pbs_python;hook_type is site",
                   "Hook;pbs_python;requestor is pbs_mom"]
        for host, mom in self.moms.items():
            for msg in log_msg:
                mom.log_match(msg, starttime=sub_time, max_attempts=60)
                _msg = "Got expected log_msg:%s on host:%s" % (msg, host)
                self.logger.info(_msg)

    @tags('container_runnable')
    @tags(SMOKE, 'hooks')
    def test_resource_list_prologue(self):
        """
        Make Resource_List available in prologue
        """
        a = {'event': 'execjob_epilogue', 'enabled': 'True'}
        rv = self.server.create_import_hook('test_resc_used', a, self.testhook,
                                            overwrite=True)
        self.assertTrue(rv)
        self.server.log_match(".*successfully sent hook file.*" +
                              "test_resc_used.PY" + ".*", regexp=True,
                              max_attempts=100, interval=5)

        rv = self.server.add_resource('fooS', type='long', flag='q')
        self.assertTrue(rv)

        j = Job(TEST_USER, attrs={'Resource_List.select': '1:ncpus=1'})
        jid = self.server.submit(j)

        a = {'job_state': 'R', 'substate': 42}
        self.server.expect(JOB, a, id=jid, attrop=PTL_AND, interval=1)

        self.server.delete(jid, wait=True)
        self.server.log_match("resources_used.fooS=10",
                              starttime=self.server.ctime)
        self.server.manager(MGR_CMD_DELETE, RSC, id="fooS")

    @requirements(num_moms=2)
    def test_execjob_preterm_with_user(self):
        """
        Run preterm hook with user=pbsuser
        """
        self.momA = (self.moms.values())[0]
        self.momB = (self.moms.values())[1]

        self.hostA = self.momA.shortname
        self.hostB = self.momB.shortname

        self.hostAfull = self.momA.hostname
        self.hostBfull = self.momB.hostname

        if self.momA.is_cpuset_mom():
            self.hostA = self.hostA + '[0]'
        if self.momB.is_cpuset_mom():
            self.hostB = self.hostB + '[0]'

        # enable job_history
        a = {'job_history_enable': 'True'}
        self.server.manager(MGR_CMD_SET, SERVER, a)

        # Create and import hook contents
        a = {'event': 'execjob_preterm', 'enabled': 'True'}
        hook_name = "test"

        hook_body = """import pbs
import os
import sys
def print_attribs(pbs_obj):
   for a in pbs_obj.attributes:
      v = getattr(pbs_obj, a)
      if (v != None) and str(v) != "":
         pbs.logmsg(pbs.LOG_DEBUG, "%s = %s" % (a,v))
e = pbs.event()
pbs.logmsg(pbs.LOG_DEBUG, "printing pbs.event() values -------->")
if e.type == pbs.EXECJOB_PRETERM:
   pbs.logmsg(pbs.LOG_DEBUG, "event is %s" % ("EXECJOB_PRETERM"))
else:
   pbs.logmsg(pbs.LOG_DEBUG, "event is %s" % ("UNKNOWN"))
pbs.logmsg(pbs.LOG_DEBUG, "hook_name is %s" % (e.hook_name))
pbs.logmsg(pbs.LOG_DEBUG, "hook_type is %s" % (e.hook_type))
pbs.logmsg(pbs.LOG_DEBUG, "requestor is %s" % (e.requestor))
pbs.logmsg(pbs.LOG_DEBUG, "requestor_host is %s" % (e.requestor_host))
pbs.logmsg(pbs.LOG_DEBUG, "printing pbs.event().job  ----------------->")
print_attribs(e.job)
pbs.logmsg(pbs.LOG_DEBUG, "Executing User=%s\
-------->" % (os.popen('whoami').read().rstrip()))
# Setting job attributes
e.job.Variable_List["BONJOUR"] = "Mounsieur Shlomi"

if e.job.in_ms_mom():
        pbs.logmsg(pbs.LOG_DEBUG, "job is in_ms_mom")
else:
        pbs.logmsg(pbs.LOG_DEBUG, "job is NOT in_ms_mom")

# Getting/setting vnode_list
vn = pbs.event().vnode_list
local_node = pbs.get_local_nodename()
for k in vn.keys():
   pbs.logmsg(pbs.LOG_DEBUG, "vn[%s]-------------->" % (k))
   print_attribs(vn[k])
   if (local_node in k):
       vn[k].comment = "In preterm hook"
       vn[k].resources_available["file"] = pbs.size("1gb")
"""
        # Create hook and import hook content
        rv = self.server.create_import_hook(hook_name, a, hook_body)
        msg = "Error while creating and importing hook contents"
        self.assertTrue(rv, msg)
        msg = "Hook %s created and " % hook_name
        msg += "hook script is imported successfully"
        self.logger.info(msg)

        # Ensure pbsuser is not a manager and operator
        svr_opr = self.server.status(SERVER, 'operators')[0]['operators']\
            .split(",")
        self.assertNotIn(str(TEST_USER), svr_opr)
        svr_mgr = self.server.status(SERVER, 'managers')[0]['managers']\
            .split(",")
        self.assertNotIn(str(TEST_USER), svr_mgr)

        # Set hook user to pbsuser
        attrs = {'user': TEST_USER}
        self.server.manager(MGR_CMD_SET, HOOK, attrs, hook_name)

        # Submit Job and check state of the job to be R
        select_stmt = "vnode=%s+" % self.hostA
        select_stmt += "vnode=%s:mem=4mb" % self.hostB
        attrib = {'Resource_List.select': select_stmt}
        j = Job(attrs=attrib)
        j.create_script("pbsdsh -n 1 /bin/date\nsleep 30")
        jid = self.server.submit(j)
        self.server.expect(JOB, {'job_state': 'R'}, id=jid)
        self.stime = time.time()
        self.server.delete(jid, runas=TEST_USER)
        # Check 'Varibale_List' job attribute
        a = {'Variable_List': (MATCH_RE, ".*BONJOUR=Mounsieur Shlomi.*")}
        self.server.expect(JOB, a, extend='x', id=jid)

        # Check  attribute on both the nodes
        nattrib = {'resources_available.file': '1gb',
                   'comment': 'In preterm hook'}
        for mom in self.moms.values():
            self.server.expect(NODE, nattrib, op=UNSET, id=mom.shortname)
        msg = "resources_available.file and comment "
        msg += "attributes of the nodes are not set"
        msg += "via hook as expected."
        self.logger.info(msg)

        # Check log msgs on both the moms
        log_msg = ["Hook;pbs_python;event is EXECJOB_PRETERM",
                   "Hook;pbs_python;hook_name is test",
                   "Hook;pbs_python;hook_type is site",
                   "Hook;pbs_python;requestor is pbs_mom",
                   "Hook;pbs_python;Executing User=%s" % (TEST_USER)]

        for mom in self.moms.values():
            for msg in log_msg:
                mom.log_match(msg, starttime=self.stime)

        # Check log msgs on server
        var1 = str(TEST_USER) + '@' + self.hostAfull
        var2 = str(TEST_USER) + '@' + self.hostBfull
        msg = "Not allowed to update vnodes or to request scheduler "
        msg += "restart cycle, if run as "
        msg += "a non-manager/operator user %s" % var1
        self.server.log_match(msg, starttime=self.stime)
        msg = "Not allowed to update vnodes or to request scheduler "
        msg += "restart cycle, if run as "
        msg += "a non-manager/operator user %s" % var2
        self.server.log_match(msg, starttime=self.stime)

        # Add pbsuser@hostB to the server's managers list
        manager = (INCR, str(TEST_USER) + '@' + self.hostBfull)
        attrs = {'managers': manager}

        self.server.manager(MGR_CMD_SET, SERVER, attrs)
        # Submit Job and check state of the job to be R
        j = Job(attrs=attrib)
        j.create_script("pbsdsh -n 1 /bin/date\nsleep 30")
        jid = self.server.submit(j)
        self.server.expect(JOB, {'job_state': 'R'}, id=jid, offset=3)
        self.stime = time.time()
        self.server.delete(jid, runas=TEST_USER)
        for mom in self.moms.values():
            for msg in log_msg:
                mom.log_match(msg, starttime=self.stime)

        # Check attribute on both the nodes
        self.server.expect(NODE, nattrib, op=UNSET, id=self.hostA)
        msg = "As expected no attribute set on node as "
        msg += "resources_available.file and comment" + self.hostA
        self.logger.info(msg)
        msg = "Din't get expected value on " + self.hostB
        self.server.expect(NODE, nattrib, op=SET, id=self.hostB)
        msg = "Got expected value for attribute 'resources_available.file'"
        msg += " and comment on node" + self.hostB
        self.logger.info(msg)

        # Check log msgs on server
        msg = "Not allowed to update vnodes or to request scheduler "
        msg += "restart cycle, if run as "
        msg += "a non-manager/operator user %s" % var1
        self.server.log_match(msg, starttime=self.stime)
        msg2 = "Updated vnode %s's attribute " % (self.hostB)
        msg2 += "comment=In preterm hook per mom hook request"
        msg3 = "Updated vnode %s's resource " % (self.hostB)
        msg3 += "resources_available.file=1gb per mom hook request"
        msg = [msg2, msg3]
        for i in msg:
            self.server.log_match(i, starttime=self.stime)
            self.logger.info("Got expected logs in server as %s", i)

        # Add pbsuser@hostA to the server's operator list
        attrs = {'operators': str(TEST_USER) + '@' + self.hostAfull}
        self.server.manager(MGR_CMD_SET, SERVER, attrs, runas=ROOT_USER)

        # Submit Job and check state of the job to be R
        j = Job(attrs=attrib)
        j.create_script("pbsdsh -n 1 /bin/date\nsleep 30")
        jid = self.server.submit(j)
        self.server.expect(JOB, {'job_state': 'R'}, id=jid, offset=3)
        self.stime = time.time()
        self.server.delete(jid, runas=TEST_USER)
        for mom in self.moms.values():
            for msg in log_msg:
                mom.log_match(msg, starttime=self.stime)

        # Check attribute on both node
        for mom in [self.hostA, self.hostB]:
            self.server.expect(NODE, nattrib, id=mom)
        msg = "Got expected value for attribute 'resources_available.file'"
        msg += " and comment on nodes"
        self.logger.info(msg)

        # Check log msgs on server
        msg1 = "Updated vnode %s's attribute " % (self.hostA)
        msg1 += "comment=In preterm hook per mom hook request"
        msg2 = "Updated vnode %s's resource " % (self.hostA)
        msg2 += "resources_available.file=1gb per mom hook request"
        msg3 = "Updated vnode %s's attribute " % (self.hostB)
        msg3 += "comment=In preterm hook per mom hook request"
        msg4 = "Updated vnode %s's resource " % (self.hostB)
        msg4 += "resources_available.file=1gb per mom hook request"
        msg = [msg1, msg2, msg3, msg4]
        for i in msg:
            self.server.log_match(i, starttime=self.stime)
            self.logger.info("Got expected logs in server as %s", i)

    def tearDown(self):
        TestFunctional.tearDown(self)
        os.environ['PBS_CONF_FILE'] = self.pbs_conf_path
        self.logger.info("Successfully exported PBS_CONF_FILE variable")
