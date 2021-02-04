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
from ptl.utils.pbs_logutils import PBSLogUtils
import re
WAIT_TIME = 20
RANGE = 15
INITIAL_TIME = 0
INELIGIBLE_TIME = 1
ELIGIBLE_TIME = 2
RUN_TIME = 3


class TestEligibleTime_ext(TestFunctional):
    """
    Test suite for eligible time with queue attribute.
    """
    pscript = ''
    lic_info = None

    def setUp(self):
        """
        Custom setup
        """
        TestFunctional.setUp(self)
        a = {'eligible_time_enable': 'True', "log_events": 2047,
             'job_history_enable': 'True'}
        self.server.manager(MGR_CMD_SET, SERVER, a)
        self.num_ncpus = '12'
        self.server.manager(MGR_CMD_SET, NODE,
                            {'resources_available.ncpus': self.num_ncpus},
                            self.mom.shortname)
        # create queue
        a = {'queue_type': 'Execution',
             'started': 'True',
             'enabled': 'True',
             'Priority': 159}
        self.server.manager(MGR_CMD_CREATE, QUEUE, a, "highp")
        self.python_path = self.du.which(hostname=self.mom.hostname,
                                         exe="python3")
        shebang_line = '#!'
        shebang_line += self.python_path
        self.pscript = [shebang_line]
        self.pscript += ['#PBS -koe']
        self.pscript += ['import os']
        self.pscript += ['import time']
        self.pscript += ['for i in range(1000):']
        self.pscript += ['    time.sleep(1)']
        self.pscript += ['    os.system("hostname")']
        self.pscript = '\n'.join(self.pscript)
        self.mom.add_config({'$min_check_poll': 5, '$max_check_poll': 15})

    def log_check(self, jid, from_acrtype='eligible_time',
                  to_acrtype='run_time', preempt_order=None, vtime=None,
                  op=None, from_jid=None, to_jid=None):
        """
        Method to verify the server log message when accrue type changes;
        by matching applicable log messages needed to calculate time
        duration to be verified in the server log message. The duration
        is verified for positive and negative value of RANGE.

        :param jid: The job id whose log message is to be verified
        :type jid: str
        :param from_acrtype: Which accrue_type the job changes from
        :type from_acrtype: str
        :param to_acrtype: The accrue_type the job changes to
        :type to_acrtype: str
        :param preempt_order: The type of preempt_order set during this
                              verification
        :type preempt_order: str
        :param vtime: The start time from where the log messages are to be
                      searched
        :type vtime: str
        :param op: User who triggered the accrue type change of the job
        :type op: str
        :param from_jid: The relevant job id's first log message to extract
                         start time of time being measured
        :type from_jid: str
        :param to_jid: The relevant job id's second log message to extract
                       end time of time being measured
        :type to_jid: str
        """
        etime = 'eligible_time'
        itime = 'ineligible_time'
        if from_jid is None and to_jid is None:
            if from_acrtype == 'eligible_time' and to_acrtype == 'exit_time':
                from_msg = jid + ";Job Queued at request of " + str(TEST_USER)
                from_msg += "@" + self.server.hostname
                self.logger.info(from_msg)
                to_msg = jid + ";delete job request received"
                self.logger.info(to_msg)
                v_msg = re.escape(jid) + ";Accrue type has changed to exiting,"
                v_msg += " previous accrue type was eligible_time"
            elif from_acrtype == 'run_time' and to_acrtype == 'exit_time':
                from_msg = jid + ";Job Run at request of Scheduler"
                self.logger.info(from_msg)
                to_msg = jid + ";delete job request received"
                self.logger.info(to_msg)
                v_msg = re.escape(jid) + ";Accrue type has changed to exiting,"
                v_msg += " previous accrue type was run_time"
            elif from_acrtype == itime and to_acrtype == 'run_time':
                from_msg = jid + ";Job Queued at request of " + str(TEST_USER)
                from_msg += "@" + self.server.hostname
                to_msg = jid + ";Job Run at request of Scheduler"
                v_msg = re.escape(jid) + ";Accrue type has changed to run_time"
                v_msg += ", previous accrue type was ineligible_time"
            elif from_acrtype == itime and to_acrtype == etime:
                from_msg = jid + ";Job Queued at request of " + str(TEST_USER)
                from_msg += "@" + self.server.hostname
                to_msg = jid + ";Job moved to "
                v_msg = re.escape(jid) + ";Accrue type has changed to "
                v_msg += "eligible_time, previous accrue type was "
                v_msg += "ineligible_time"
            elif from_acrtype == 'eligible_time' and to_acrtype == 'run_time':
                if preempt_order == 'suspend':
                    if op == str(OPER_USER) or op == str(MGR_USER):
                        from_msg = jid + ";job signaled with suspend by " + op
                        to_msg = jid + ";job signaled with resume by Scheduler"
                        to_msg += "@" + self.server.hostname
                    else:
                        from_msg = jid + ";job signaled with suspend by"
                        from_msg += " Scheduler@" + self.server.hostname
                        to_msg = jid + ";job signaled with resume by "
                        to_msg += "Scheduler@" + self.server.hostname
                elif preempt_order == 'requeue':
                    from_msg = jid + ";Job Rerun at request of Scheduler"
                    from_msg += "@" + self.server.hostname
                    to_msg = jid + ";Job Run at request of Scheduler"
                    to_msg += "@" + self.server.hostname
                else:
                    from_msg = jid + ";Job Queued at request of "
                    from_msg += str(TEST_USER) + "@" + self.server.hostname
                    to_msg = jid + ";Job Run at request of Scheduler"
                v_msg = re.escape(jid) + ";Accrue type has changed to run_time"
                v_msg += ", previous accrue type was eligible_time"
        else:
            if not from_jid:
                from_jid = jid
            elif not to_jid:
                to_jid = jid
            if from_acrtype == etime and to_acrtype == itime:
                from_msg = from_jid + \
                    ";Job Queued at request of " + str(TEST_USER)
                from_msg += "@" + self.server.hostname
                to_msg = to_jid + ";Job Run at request of Scheduler"
                to_msg += "@" + self.server.hostname
                v_msg = re.escape(jid) + ";Accrue type has changed to "
                v_msg += "ineligible_time, previous accrue type was"
                v_msg += " eligible_time"
            elif from_acrtype == 'run_time' and to_acrtype == 'exit_time':
                from_msg = from_jid + \
                    ";Job Queued at request of " + str(TEST_USER)
                from_msg += "@" + self.server.hostname
                self.logger.info(from_msg)
                to_msg = to_jid + ";Job Run at request of Scheduler"
                to_msg += "@" + self.server.hostname
                self.logger.info(to_msg)
                v_msg = re.escape(jid) + ";Accrue type has changed to exiting,"
                v_msg += " previous accrue type was run_time"
            elif from_acrtype == 'eligible_time' and to_acrtype == 'run_time':
                from_msg = from_jid + \
                    ";Job Queued at request of " + str(TEST_USER)
                from_msg += "@" + self.server.hostname
                to_msg = to_jid + ";Job Run at request of Scheduler"
                to_msg += "@" + self.server.hostname
                v_msg = re.escape(jid) + ";Accrue type has changed to run_time"
                v_msg += ", previous accrue type was eligible_time"

        m1 = self.server.log_match(from_msg, starttime=vtime)
        self.logger.info(m1)
        t1 = PBSLogUtils.convert_date_time(m1[1].split(';')[0])
        self.logger.info(t1)

        m2 = self.server.log_match(to_msg, starttime=vtime)
        self.logger.info(m2)
        t2 = PBSLogUtils.convert_date_time(m2[1].split(';')[0])
        self.logger.info(t2)
        verif_time = int(t2) - int(t1)

        (x, y) = self.server.log_match(v_msg, max_attempts=20, regexp=True,
                                       starttime=vtime)
        log_str = y.split(" ")
        indx = log_str.index("secs,")
        elg_time = log_str[indx - 1]
        diff = abs(verif_time - int(elg_time))
        msg = "Difference of expected and actual eligible_time"
        self.logger.info(msg + " is %s" % diff)
        self.assertLess(diff, RANGE)

    def test_eligible_time_with_reservation(self):
        """
        Changing a queue start attribute should only affect eligible_time of
        jobs in that queue
        """
        now = time.time()

        a = {'reserve_start': now + 3600, 'reserve_end': now + 7200}
        r = Reservation(TEST_USER, attrs=a)
        rid = self.server.submit(r)
        rq = rid.split('.')[0]
        a = {'reserve_state': (MATCH_RE, "RESV_CONFIRMED|2")}
        self.server.expect(RESV, a, id=rid)

        a = {'scheduling': 'False'}
        self.server.manager(MGR_CMD_SET, SERVER, a)

        j = Job()
        j.set_sleep_time(1000)
        jid = self.server.submit(j)
        self.server.expect(JOB, {'accrue_type': ELIGIBLE_TIME}, id=jid)

        j = Job(attrs={ATTR_queue: rq})
        jid2 = self.server.submit(j)

        a = {'started': 'True'}
        self.server.manager(MGR_CMD_SET, QUEUE, a, id=rq)

        self.server.expect(JOB, {'accrue_type': ELIGIBLE_TIME}, id=jid2)
        self.server.expect(JOB, {'accrue_type': ELIGIBLE_TIME}, id=jid)

    @skipOnCpuSet
    def test_eligible_time_attributes(self):
        """
        Test to verify visibility and settings of attributes
        eligible_time_enable, eligible_time & accrue_type
        """
        ulist = [str(TEST_USER), str(MGR_USER), str(OPER_USER)]

        # 1. Verification of visibility of eligible_time_enable
        # server attribute
        for u in ulist:
            attr_dict = self.server.status(runas=u)[0]
            self.assertIn('eligible_time_enable', attr_dict, "Failed to get" +
                          " status when run as " + u)
            self.assertEqual(attr_dict['eligible_time_enable'], 'True')

        # 2. Verification of setting of eligible_time_enable server attribute
        a = {'eligible_time_enable': False}
        try:
            self.server.manager(MGR_CMD_SET, SERVER, a, runas=MGR_USER)
        except PbsManagerError as e:
            raise self.fail("Unexpected error when" +
                            "attribute is set as manager")
        attr_dict = self.server.status(runas=u)[0]
        self.assertIn('eligible_time_enable', attr_dict)
        self.assertEqual(attr_dict['eligible_time_enable'], 'False')

        oper_msg = 'Cannot set attribute, read only or '
        oper_msg += 'insufficient permission'
        usr_msg = 'Unauthorized Request'
        udic = {
            'oper': oper_msg,
            'usr': usr_msg
        }
        for k, v in udic.items():
            a = {'eligible_time_enable': False}
            f_msg = "Failed to get expected message "
            runas = None
            if k == 'oper':
                runas = OPER_USER
            elif k == 'usr':
                runas = TEST_USER
            with self.assertRaises(PbsManagerError, msg=f_msg + v) as e:
                self.server.manager(MGR_CMD_SET, SERVER, a, runas=runas)
            self.assertIn(v, e.exception.msg[0])

        a = {'eligible_time_enable': True}
        self.server.manager(MGR_CMD_SET, SERVER, a, runas=MGR_USER)

        jlist = self.common_submission()

        # 3. Verification of visibility of eligible_time job attribute
        # adding a lag of 10s for slower systems
        for u in ulist:
            self.check_eligible_time(start=WAIT_TIME, jid=jlist[1], runas=u)

        # 4. Verification of setting of eligible_time job attribute only
        # as manager or operator
        fmsg = "Failed to get error message: "
        msg = 'Cannot set attribute, read only or insufficient '
        msg += 'permission  eligible_time'
        with self.assertRaises(PbsAlterError, msg=fmsg+msg) as e:
            self.server.alterjob(jlist[1],
                                 {ATTR_W: 'eligible_time=00:03:00'},
                                 runas=str(TEST_USER))
        self.assertIn(msg, e.exception.msg[0])
        self.server.cleanup_jobs()

        for u in [MGR_USER, OPER_USER]:
            jlist = self.common_submission()
            now = time.time()
            try:
                self.server.alterjob(jlist[1],
                                     {ATTR_W: 'eligible_time=00:03:00'},
                                     runas=u)
            except PbsAlterrError as e:
                msg = "Failed to alter Eligible time of job"
                self.fail(msg)
            self.check_eligible_time(start=180, jid=jlist[1], runas=u)
            v_msg = jlist[1] + ";Accrue type is eligible_time, "
            v_msg += "previous accrue type was eligible_time for "
            v_msg += "2[0-9] secs, due to qalter total eligible_time="
            verify_msg = v_msg + '00:03:00'
            self.server.log_match(verify_msg, max_attempts=2,
                                  starttime=now, regexp=True)
            self.server.cleanup_jobs()

        # 5. Visibility verification of accrue_type job attribute only
        # as manager and operator
        jlist = self.common_submission()
        for u in ulist:
            try:
                self.server.expect(JOB, {'accrue_type': ELIGIBLE_TIME},
                                   id=jlist[1], runas=u, max_attempts=5)
            except PtlExpectError as e:
                if u == MGR_USER:
                    emsg = "Unexpected error when attribute is viewed as "
                    self.fail(emsg + u)
                elif u == str(TEST_USER) or u == OPER_USER:
                    pass

        # 6. Verification of setting of accrue_type job attribute not
        # being allowed
        for u in ulist:
            fmsg = "Failed to get error message: "
            msg = 'Cannot set attribute, read only or insufficient '
            msg += 'permission  accrue_type'
            with self.assertRaises(PbsAlterError, msg=fmsg+msg) as e:
                self.server.alterjob(jlist[1], {ATTR_W: 'accrue_type=3'},
                                     runas=u)
            self.assertIn(msg, e.exception.msg[0])

    @skipOnCpuSet
    def test_eligible_time_server_restart(self):
        """
        Test to verify that there is no impact of server restarts on
        eligible_time of jobs
        """

        rs = ['signal', 'init_script']
        for t in rs:
            a = {ATTR_l + '.select': '1:ncpus=' + self.num_ncpus,
                 ATTR_k: 'oe'}
            J1 = Job(attrs=a)
            J1.set_sleep_time(500)
            jid = self.server.submit(J1)
            self.server.expect(JOB, {ATTR_state: 'R'}, id=jid)

            a = {ATTR_l + '.select': '1:ncpus=' + self.num_ncpus,
                 ATTR_k: 'oe'}
            J2 = Job(attrs=a)
            J2.set_sleep_time(500)
            jid2 = self.server.submit(J2)
            self.server.expect(JOB, {ATTR_state: 'Q'}, id=jid2)

            a = {ATTR_l + '.select': '1:ncpus=' + self.num_ncpus,
                 ATTR_k: 'oe'}
            J3 = Job(attrs=a)
            J3.set_sleep_time(500)
            jid3 = self.server.submit(J3)
            self.server.expect(JOB, {ATTR_state: 'Q'}, id=jid3)

            self.logger.info('Waiting for eligible_time to accrue')
            time.sleep(WAIT_TIME)
            self.check_eligible_time(start=WAIT_TIME, jid=jid2)
            self.check_eligible_time(start=WAIT_TIME, jid=jid3)
            self.server.deljob(jid)
            self.server.expect(JOB, {ATTR_state: 'R'}, id=jid2)
            self.server.expect(JOB, {ATTR_state: 'Q'}, id=jid3)

            self.logger.info('Waiting for eligible_time to accrue')
            time.sleep(WAIT_TIME)
            self.check_eligible_time(start=WAIT_TIME, jid=jid2)
            self.check_eligible_time(start=int(WAIT_TIME)*2, jid=jid3)
            now1 = time.time()
            if t == 'signal':
                self.server.signal('-KILL')
                self.server.start()
            elif t == 'init_script':
                self.server.restart()
            self.logger.info('Waiting for server to restart')
            self.server.isUp()
            now2 = time.time()
            ndiff = int(now2 - now1)
            self.server.expect(JOB, {ATTR_state: 'R'}, id=jid2)
            self.server.expect(JOB, {ATTR_state: 'Q'}, id=jid3)
            self.check_eligible_time(start=WAIT_TIME, jid=jid2, offset=15)
            s = WAIT_TIME * 2 + ndiff
            self.check_eligible_time(start=s, jid=jid3)
            self.server.delete([jid2, jid3])

    def common_submission(self, queued=1):
        """
        Common job submission - 1 running job N queued job with default
        WAIT_TIME seconds
        :returns: List of two job ids submitted
        """
        jid_list = []
        a = {ATTR_l + '.select': '1:ncpus=' + self.num_ncpus, ATTR_k: 'oe'}
        J1 = Job(attrs=a)
        J1.set_sleep_time(500)
        jid_list.append(self.server.submit(J1))
        self.server.expect(JOB, {ATTR_state: 'R'}, id=jid_list[0])

        for i in range(0, queued):
            a = {ATTR_l + '.select': '1:ncpus=1', ATTR_k: 'oe'}
            J2 = Job(attrs=a)
            J2.set_sleep_time(500)
            jid_list.append(self.server.submit(J2))
        self.logger.info('Waiting for eligible_time to accrue')
        time.sleep(WAIT_TIME)
        for i in range(1, queued):
            self.server.expect(JOB, {ATTR_state: 'Q'}, id=jid_list[i])
        return jid_list

    @skipOnCpuSet
    def test_eligible_time_deleted_job(self):
        """
        Test to verify the eligible_time when queued job and running job
        is deleted
        """
        jlist = []
        jlist = self.common_submission()
        self.check_eligible_time(jid=jlist[1])
        self.server.expect(JOB, {'accrue_type': ELIGIBLE_TIME}, id=jlist[1])
        # Verify eligible time & accrue_type of a deleted queued job
        self.server.deljob(jlist[1])
        self.check_eligible_time(jid=jlist[1], extend='x')
        self.log_check(jid=jlist[1], from_acrtype='eligible_time',
                       to_acrtype='exit_time')

        self.server.expect(JOB, {'accrue_type': 4}, id=jlist[1], extend='x')
        self.server.expect(JOB, {'accrue_type': RUN_TIME}, id=jlist[0],
                           extend='x')

        # Verify eligible_time & accrue_type of a deleted running job
        self.server.deljob(jlist[0])
        self.check_eligible_time(start=0, jid=jlist[0], extend='x')
        self.log_check(jid=jlist[0], from_acrtype='run_time',
                       to_acrtype='exit_time')

        self.server.expect(JOB, {'accrue_type': 4}, id=jlist[0], extend='x')

    @skipOnCpuSet
    def test_eligible_time_varying_settings(self):
        """
        Test to verify the eligible_time of a job set in below cases:
        a. set to a value less than the job's accrued eligible time
        b. set to zero
        c. set to a value after job stopped accruing eligible time
        """
        values = ['00:00:10', '00:00:00', '00:02:00', '-00:01:00']
        for t in values:
            jlist = []
            et = 0
            jlist = self.common_submission()
            self.check_eligible_time(jid=jlist[1])
            self.server.expect(JOB, {'accrue_type': ELIGIBLE_TIME},
                               id=jlist[1])
            if t != '00:02:00':
                try:
                    self.server.alterjob(jlist[1], {ATTR_W: 'eligible_time=' +
                                         t})
                except PbsAlterError as e:
                    if t == '-00:01:00':
                        msg = "Illegal attribute or resource value"
                        self.assertIn(msg, e.msg[0])
                        continue
                    else:
                        raise self.fail("Unexpected error when " +
                                        "attribute is set")
                if t == '00:00:00':
                    et = 0
                elif t == '00:00:10':
                    et = 10
                self.check_eligible_time(start=et, jid=jlist[1])
                # Server log verification
                self.logger.info('Waiting for eligible_time to accrue')
                time.sleep(WAIT_TIME)
                self.check_eligible_time(start=et+WAIT_TIME, jid=jlist[1])
                v_msg = jlist[1] + ";Accrue type is eligible_time, "
                v_msg += "previous accrue type was eligible_time for "
                v_msg += "2[0-9] secs, due to qalter total eligible_time="
                verify_msg = v_msg + t
                self.server.log_match(verify_msg, max_attempts=2,
                                      regexp=True)
            self.server.deljob(jlist[0])
            self.server.expect(JOB, {ATTR_state: 'R'}, id=jlist[1])
            # Server log verification
            if t == '00:02:00':
                self.server.alterjob(jlist[1], {ATTR_W: 'eligible_time=' + t})
                self.check_eligible_time(start=120, jid=jlist[1])
                v_msg = jlist[1] + ";Accrue type is run_time, "
                v_msg += "previous accrue type was run_time for "
                v_msg += "[0-9] secs, due to qalter total eligible_time="
                verify_msg = v_msg + t
                self.server.log_match(verify_msg, max_attempts=2,
                                      regexp=True)
            self.server.deljob(jlist[1])

    @skipOnCpuSet
    def test_eligible_time_enabling_time(self):
        """
        Test to verify the eligible_time is accrued only after enabling
        eligible_time_enable server attribute
        """
        a = {'eligible_time_enable': False}
        self.server.manager(MGR_CMD_SET, SERVER, a)

        jlist = []
        jlist = self.common_submission()
        try:
            self.server.expect(JOB, {'eligible_time': 30}, op=LT, id=jlist[1],
                               max_attempts=2)
        except PtlExpectError as e:
            pass
        try:
            self.server.expect(JOB, {'accrue_type': ELIGIBLE_TIME},
                               id=jlist[1], max_attempts=2)
        except PtlExpectError as e:
            pass
        a = {'eligible_time_enable': True}
        self.server.manager(MGR_CMD_SET, SERVER, a)
        self.logger.info('Waiting for eligible_time to accrue')
        time.sleep(WAIT_TIME)
        self.check_eligible_time(start=WAIT_TIME, jid=jlist[1])
        self.server.expect(JOB, {'accrue_type': ELIGIBLE_TIME}, id=jlist[1])

    def check_eligible_time(self, jid, start=WAIT_TIME, offset=RANGE,
                            extend=None, runas=MGR_USER):
        """
        Method to verify eligible_time of a job to be within given range
        of seconds (positive and negative)

        :param jid: The job id whose eligible_time is to be verified
        :type jid: str
        :param start: The start of time range in seconds from where we need to
                      verify the eligible_time of the job
        :type start: int
        :param offset: The offset in seconds specifying the range (+ & -)
                       uptill which the eligible_time of the job is verified
        :type offset: int
        :param extend: The additional option to be specified while querying
                       eligible_time of the job
        :type extend: str
        :param runas: Query command of eligible_time of the job to be runas
        :type runas: str

        :returns: returns the eligible time of the job in seconds
        """
        qstat = self.server.status(JOB, 'eligible_time', id=jid, extend=extend,
                                   runas=runas)
        e_time = qstat[0]['eligible_time']
        ce_time = PBSLogUtils.convert_hhmmss_time(e_time)
        self.logger.info("eligible_time of '%s' is '%d'" % (jid, ce_time))
        diff = abs(start - ce_time)
        msg = "Difference of expected and actual eligible_time attribute"
        self.logger.info(msg + " is %s" % diff)
        self.assertLessEqual(diff, offset)
        return ce_time

    @skipOnCpuSet
    def test_ineligible_job_moved_to_eligible(self):
        """
        Test to verify the ineligible_time getting accrued by job which exceeds
        limits on one queue and gets eligible when moved to other queue
        """
        queue_attrib = {ATTR_qtype: 'execution',
                        ATTR_start: 'True',
                        ATTR_enable: 'True'}
        self.server.manager(MGR_CMD_CREATE, QUEUE, queue_attrib, id='workq1')
        self.server.manager(MGR_CMD_SET, QUEUE,
                            {'max_run': '[u:PBS_GENERIC=1]'},
                            id='workq')
        jid_list = []
        a = {ATTR_l + '.select': '1:ncpus=' + self.num_ncpus, ATTR_k: 'oe'}
        J1 = Job(attrs=a)
        J1.set_sleep_time(500)
        jid_list.append(self.server.submit(J1))
        self.server.expect(JOB, {ATTR_state: 'R'}, id=jid_list[0])

        a = {ATTR_l + '.select': '1:ncpus=1', ATTR_k: 'oe'}
        J2 = Job(attrs=a)
        J2.set_sleep_time(500)
        jid_list.append(self.server.submit(J2))
        self.server.expect(JOB, {ATTR_state: 'Q'}, id=jid_list[1])
        self.server.expect(JOB, {'accrue_type': INELIGIBLE_TIME},
                           id=jid_list[1])
        self.logger.info('Waiting for eligible_time to accrue')
        time.sleep(WAIT_TIME)
        self.server.expect(JOB, {ATTR_state: 'Q'}, id=jid_list[1])
        self.check_eligible_time(start=0, jid=jid_list[1], offset=1)
        self.server.movejob(jid_list[1], "workq1")
        self.server.expect(JOB, {ATTR_state: 'Q'}, id=jid_list[1])
        self.server.expect(JOB, {'accrue_type': ELIGIBLE_TIME}, id=jid_list[1])
        self.logger.info('Waiting for eligible_time to accrue')
        time.sleep(WAIT_TIME)
        self.check_eligible_time(start=WAIT_TIME, jid=jid_list[1])
        self.log_check(jid=jid_list[1], from_acrtype='ineligible_time',
                       to_acrtype='eligible_time')

    @skipOnCpuSet
    def test_eligible_time_moved_job(self):
        """
        Test to verify the eligible_time getting accrued is continued for job
        that is moved from one queue to another
        """
        queue_attrib = {ATTR_qtype: 'execution',
                        ATTR_start: 'True',
                        ATTR_enable: 'True'}
        self.server.manager(MGR_CMD_CREATE, QUEUE, queue_attrib, id='workq1')

        jlist = []
        jlist = self.common_submission()
        self.check_eligible_time(jid=jlist[1])
        self.server.movejob(jlist[1], "workq1")
        self.logger.info('Waiting for eligible_time to accrue')
        time.sleep(WAIT_TIME)
        self.check_eligible_time(start=WAIT_TIME * 2, jid=jlist[1])

    @skipOnCpuSet
    def test_eligible_time_prime_nonprime(self):
        """
        Test to verify the eligible_time getting accrued is continued for job
        queued in prime time and crossing prime-non prime border and
        vice-versa
        """
        queue_attrib = {ATTR_qtype: 'execution',
                        ATTR_start: 'True',
                        ATTR_enable: 'True'}
        self.server.manager(MGR_CMD_CREATE, QUEUE, queue_attrib, id='p_queue')
        self.server.manager(MGR_CMD_CREATE, QUEUE, queue_attrib, id='np_queue')
        prime_offset = 80
        nonprime_offset = 200
        # Delete all entries in the holidays file
        self.scheduler.holidays_delete_entry('a')

        lt = time.localtime(time.time())
        self.scheduler.holidays_set_year(str(lt[0]))

        now = int(time.time())
        prime = time.strftime('%H%M', time.localtime(now + prime_offset))
        nonprime = time.strftime('%H%M', time.localtime(now + nonprime_offset))

        # set prime-time and nonprime-time for all days
        self.scheduler.holidays_set_day('weekday', prime, nonprime)
        self.scheduler.holidays_set_day('saturday', prime, nonprime)
        self.scheduler.holidays_set_day('sunday', prime, nonprime)

        jid_list = []

        a = {ATTR_l + '.select': '1:ncpus=1', ATTR_queue: 'p_queue',
             ATTR_k: 'oe'}
        J1 = Job(attrs=a)
        J1.set_sleep_time(50)
        jid_list.append(self.server.submit(J1))
        self.server.expect(JOB, {ATTR_state: 'Q'}, id=jid_list[0])

        self.logger.info('Waiting for prime time to start')
        time.sleep(prime_offset)
        self.check_eligible_time(start=prime_offset, jid=jid_list[0])
        self.server.expect(JOB, {ATTR_state: 'R'}, id=jid_list[0])
        # server log verification
        self.log_check(jid=jid_list[0], from_acrtype='eligible_time',
                       to_acrtype='run_time')
        self.server.deljob(jid_list[0])

        a = {ATTR_l + '.select': '1:ncpus=1', ATTR_queue: 'np_queue',
             ATTR_k: 'oe'}
        J2 = Job(attrs=a)
        J2.set_sleep_time(50)
        jid_list.append(self.server.submit(J2))
        self.server.expect(JOB, {ATTR_state: 'Q'}, id=jid_list[1])

        self.logger.info('Waiting for non-prime time to start')
        time.sleep(nonprime_offset - prime_offset)
        self.check_eligible_time(start=nonprime_offset-prime_offset,
                                 jid=jid_list[1])
        self.server.expect(JOB, {ATTR_state: 'R'}, id=jid_list[1])
        # server log verification
        self.log_check(jid=jid_list[1], from_acrtype='eligible_time',
                       to_acrtype='run_time')

    @skipOnCpuSet
    def test_eligible_time_accounting_log(self):
        """
        Test to verify the eligible_time in accounting log record
        """
        now = time.time()
        jlist = []
        jlist = self.common_submission()
        self.server.alterjob(jlist[1], {'Resource_List.walltime': 10})
        self.server.deljob(jlist[0])
        self.server.expect(JOB, {ATTR_state: 'R'}, id=jlist[1], offset=4)
        self.server.log_match("%s;Exit_status=" % jlist[1], interval=4)
        qstat = self.server.status(JOB, 'eligible_time', id=jlist[1],
                                   extend='x')
        e_time = qstat[0]['eligible_time']
        record = "E;%s;.*eligible_time=%s.*" % (jlist[1], e_time)
        self.server.accounting_match(msg=record, id=jlist[1], n='ALL',
                                     regexp=True, max_attempts=10,
                                     starttime=now)

    def test_suspend_sub_job(self):
        """
        subjobs will accrue eligible time if any subjob is suspended by
        Manager/Operator
        """
        now = time.time()
        ja = Job(attrs={ATTR_J: '1-10', ATTR_l + '.walltime': 600})
        ja.create_script(self.pscript)
        ja_id = self.server.submit(ja)
        self.server.expect(JOB, {'job_state=R': 10}, count=True,
                           id=ja_id, extend='t')
        subjid = ja.create_subjob_id(ja_id, 1)

        for u in [MGR_USER, OPER_USER]:
            now = time.time()
            self.server.sigjob(subjid, 'suspend', runas=u)
            self.check_eligible_time(jid=subjid, start=0, offset=40)
            self.server.expect(JOB, {'job_state': 'S'}, id=subjid)
            self.logger.info('Waiting for eligible_time to accrue')
            time.sleep(WAIT_TIME)
            qstat = self.server.status(JOB, 'eligible_time', id=subjid)
            e_time = qstat[0]['eligible_time']
            val1 = PBSLogUtils.convert_hhmmss_time(e_time)
            self.check_eligible_time(jid=subjid, start=val1, offset=40)
            self.server.sigjob(subjid, 'resume', runas=u)
            self.server.expect(JOB, {'job_state=R': 10}, count=True,
                               id=ja_id, extend='t')
            self.check_eligible_time(jid=subjid, start=val1, offset=40)
            for i in range(2, 10):
                subjid1 = ja.create_subjob_id(ja_id, i)
                self.server.expect(JOB, {'accrue_type': RUN_TIME}, id=subjid1)
            self.logger.info('Waiting for eligible_time to accrue')
            time.sleep(WAIT_TIME)
            self.check_eligible_time(jid=subjid, start=val1, offset=40)
            self.log_check(jid=subjid, from_acrtype='eligible_time',
                           to_acrtype='run_time', preempt_order='suspend',
                           vtime=now, op=str(u))

    def test_job_suspend(self):
        """
        job will accrue eligible time if suspended by Manager/Operator
        """
        j = Job()
        j.create_script(self.pscript)
        j_id = self.server.submit(j)
        self.server.expect(JOB, {'job_state': 'R'}, j_id)
        for u in [MGR_USER, OPER_USER]:
            self.server.sigjob(j_id, 'suspend', runas=u)
            self.check_eligible_time(jid=j_id, start=0, offset=40)
            self.logger.info('Waiting for eligible_time to accrue')
            time.sleep(WAIT_TIME)
            qstat = self.server.status(JOB, 'eligible_time', id=j_id)
            e_time = qstat[0]['eligible_time']
            val1 = PBSLogUtils.convert_hhmmss_time(e_time)
            self.check_eligible_time(jid=j_id, start=val1, offset=40)
            self.server.sigjob(j_id, 'resume', runas=u)
            self.server.expect(JOB, {'job_state': 'R'}, j_id)
            self.check_eligible_time(jid=j_id, start=val1, offset=40)

    def test_eligible_time_preempted_job(self):
        """
        job is preempted by one large high priority job
        """
        a = {'Resource_List.select': '1:ncpus=12'}
        j = Job(attrs=a)
        j.create_script(self.pscript)
        j_id = self.server.submit(j)
        self.server.expect(JOB, {'job_state': 'R'}, j_id)
        # submit a job to high priority queue
        a = {'Resource_List.select': '1:ncpus=12',
             'queue': 'highp',
             ATTR_l + '.walltime': 50}
        hj = Job(attrs=a)
        hj.create_script(self.pscript)
        hjid = self.server.submit(hj)
        self.server.expect(JOB, {'job_state': 'S'}, j_id)
        self.server.expect(JOB, {'job_state': 'R'}, id=hjid)
        self.check_eligible_time(jid=j_id, start=0, offset=40)
        self.logger.info('Waiting for eligible_time to accrue')
        time.sleep(WAIT_TIME)
        self.server.log_match("%s;Exit_status=" % hjid, interval=4)
        self.server.expect(JOB, {'job_state': 'R'}, j_id)
        qstat = self.server.status(JOB, 'eligible_time', id=j_id)
        e_time = qstat[0]['eligible_time']
        val = PBSLogUtils.convert_hhmmss_time(e_time)
        self.check_eligible_time(jid=j_id, start=val, offset=40)
        self.log_check(jid=j_id, from_acrtype='eligible_time',
                       to_acrtype='run_time', preempt_order='suspend',
                       op=str(TEST_USER))

    def test_accrue_eligible_time_after_preempted(self):
        """
        job will accrue eligible_time after being preempted and requeued
        """
        # submit two jobs to regular queue
        attrs = {'Resource_List.select': '6:ncpus=1'}
        j1 = Job(attrs=attrs)
        j1.create_script(self.pscript)
        jid1 = self.server.submit(j1)
        self.server.expect(JOB, {'job_state': 'R'}, id=jid1)
        j2 = Job(attrs=attrs)
        j2.create_script(self.pscript)
        jid2 = self.server.submit(j2)
        self.server.expect(JOB, {'job_state': 'R'}, id=jid2)
        # set preempt order
        b = {'preempt_order': 'R'}
        self.server.manager(MGR_CMD_SET, SCHED, b)
        # submit another job to regular queue
        j3 = Job()
        j3.set_sleep_time(30)
        jid3 = self.server.submit(j3)
        self.server.expect(JOB, {'job_state': 'R'}, id=jid1)
        self.server.expect(JOB, {'job_state': 'R'}, id=jid2)
        self.server.expect(JOB, {'job_state': 'Q'}, id=jid3)
        now = time.time()
        self.server.runjob(jobid=jid3)
        jds = self.server.status(JOB)
        q_jid = [job for job in jds if job['job_state'] == 'Q'][0]['id']
        self.server.expect(JOB, {'job_state': 'R'}, id=jid3)
        self.logger.info('Waiting for eligible_time to accrue')
        time.sleep(WAIT_TIME)
        self.check_eligible_time(jid=q_jid, start=WAIT_TIME, offset=10)
        self.server.expect(JOB, {'job_state': 'F'}, id=jid3, offset=20,
                           extend='x')
        self.log_check(jid=q_jid, from_acrtype='eligible_time',
                       to_acrtype='run_time', preempt_order='requeue',
                       vtime=now)

    def test_preemption_subjobs(self):
        """
        preemption for subjobs
        """
        now = time.time()
        ja1 = Job(attrs={ATTR_J: '1-12',
                  ATTR_l + '.walltime': 60})
        ja1.create_script(self.pscript)
        ja_id1 = self.server.submit(ja1)
        self.server.expect(JOB, {'job_state=R': 12}, count=True,
                           id=ja_id1, extend='t')
        self.server.expect(JOB, {'accrue_type': INELIGIBLE_TIME}, id=ja_id1)
        # submit a job to high priority queue
        a = {ATTR_q: 'highp'}
        j2 = Job(attrs=a)
        j2.set_sleep_time(30)
        j_id2 = self.server.submit(j2)
        self.server.expect(JOB, {'job_state=S': 1}, count=True, extend='t')
        sjid = self.server.status(JOB, id=ja_id1, extend='t')
        susp_job_id = [job for job in sjid if job['job_state'] == 'S'][0]['id']
        self.server.expect(JOB, {'job_state': 'R'}, id=j_id2)
        self.check_eligible_time(jid=susp_job_id, start=0, offset=40)
        self.logger.info('Waiting for eligible_time to accrue')
        time.sleep(WAIT_TIME)
        qstat = self.server.status(JOB, 'eligible_time', id=susp_job_id)
        e_time = qstat[0]['eligible_time']
        val = PBSLogUtils.convert_hhmmss_time(e_time)
        self.server.expect(JOB, {'job_state': 'F'}, id=j_id2, extend='x')
        self.server.expect(JOB, {'job_state=R': 12}, count=True,
                           id=ja_id1, extend='t')
        self.check_eligible_time(jid=susp_job_id, start=val, offset=40)
        self.server.expect(JOB, {'accrue_type': INELIGIBLE_TIME}, id=ja_id1)
        self.log_check(jid=susp_job_id, from_acrtype='eligible_time',
                       to_acrtype='run_time', preempt_order='suspend',
                       vtime=now)

    def test_ineligible_time_of_job_above_soft_limit(self):
        """
        Job accrues ineligible time after exceeding the max_run_soft
        queue limit
        """
        pre_prio = "express_queue, normal_jobs, server_softlimits,"
        pre_prio += " queue_softlimits"
        a = {'preempt_prio': pre_prio}
        self.server.manager(MGR_CMD_SET, SCHED, a)
        self.server.manager(MGR_CMD_SET, QUEUE,
                            {'max_run_soft': "[u:PBS_GENERIC=2]"},
                            "workq")
        a = {'Resource_List.select': '5:ncpus=1',
             ATTR_l + '.walltime': 100}
        j1 = Job(TEST_USER1, a)
        j1.create_script(self.pscript)
        j_id1 = self.server.submit(j1)
        time.sleep(2)
        a = {'Resource_List.select': '3:ncpus=1',
             ATTR_l + '.walltime': 100}
        j2 = Job(TEST_USER1, a)
        j2.create_script(self.pscript)
        j_id2 = self.server.submit(j2)
        time.sleep(2)
        a = {'Resource_List.select': '2:ncpus=1',
             ATTR_l + '.walltime': 100}
        j3 = Job(TEST_USER1, a)
        j3.create_script(self.pscript)
        j_id3 = self.server.submit(j3)
        time.sleep(2)

        self.server.expect(JOB, {'job_state': 'R'}, j_id1)
        self.server.expect(JOB, {'job_state': 'R'}, j_id2)
        self.server.expect(JOB, {'job_state': 'R'}, j_id3)

        j4 = Job(TEST_USER2)
        j4.create_script(self.pscript)
        j5 = Job(TEST_USER2)
        j5.create_script(self.pscript)
        j_id4 = self.server.submit(j4)
        j_id5 = self.server.submit(j5)
        self.server.expect(JOB, {'job_state': 'R'}, j_id4)
        self.server.expect(JOB, {'job_state': 'R'}, j_id5)
        j6 = Job(TEST_USER1)
        j6.create_script(self.pscript)
        j_id6 = self.server.submit(j6)
        self.server.expect(JOB, {'job_state': 'Q'}, j_id6)
        self.server.expect(JOB, {'accrue_type': INELIGIBLE_TIME}, id=j_id6)
        self.server.delete([j_id4, j_id5, j_id6], extend='force', wait=True)
        j7 = Job(TEST_USER1, a)
        j7.create_script(self.pscript)
        j_id7 = self.server.submit(j7)
        time.sleep(2)
        self.server.expect(JOB, {'job_state': 'R'}, j_id7)
        j8 = Job(TEST_USER3)
        j8.create_script(self.pscript)
        j_id8 = self.server.submit(j8)
        self.server.expect(JOB, {'job_state': 'R'}, j_id8)
        self.server.expect(JOB, {'accrue_type': RUN_TIME}, id=j_id8)
        self.server.expect(JOB, {'job_state': 'S'}, j_id7)
        self.server.expect(JOB, {'accrue_type': INELIGIBLE_TIME}, id=j_id7)

    def test_job_array_accrue_type(self):
        """
        Test job attribute:accrue_type at various stages
        during subjob preemption
        """
        a = {'scheduling': 'False'}
        self.server.manager(MGR_CMD_SET, SERVER, a)
        ja1 = Job(attrs={ATTR_J: '1-12'})
        ja1.create_script(self.pscript)
        ja_id1 = self.server.submit(ja1)
        self.server.expect(JOB, {'job_state': 'Q'}, id=ja_id1)
        self.logger.info('Waiting for eligible_time to accrue')
        time.sleep(WAIT_TIME)
        self.server.expect(JOB, {'accrue_type': ELIGIBLE_TIME}, id=ja_id1)
        self.server.manager(MGR_CMD_SET, SERVER, {'scheduling': 'True'})
        self.server.expect(JOB, {'job_state': 'B'}, id=ja_id1)
        self.logger.info('Waiting for eligible_time to accrue')
        time.sleep(WAIT_TIME)
        self.server.expect(JOB, {'job_state=R': 12}, count=True,
                           id=ja_id1, extend='t')
        self.server.expect(JOB, {'accrue_type': INELIGIBLE_TIME}, id=ja_id1)
        self.server.expect(JOB, {'accrue_type=3': 12}, count=True,
                           id=ja_id1, extend='t')
        self.check_eligible_time(start=WAIT_TIME, jid=ja_id1, offset=20)
        qstat = self.server.status(JOB, 'eligible_time', id=ja_id1)
        e_time = qstat[0]['eligible_time']
        val1 = PBSLogUtils.convert_hhmmss_time(e_time)
        now1 = time.time()
        a = {'queue': 'highp', ATTR_l + '.walltime': 100}
        hj = Job(attrs=a)
        hj.create_script(self.pscript)
        hjid = self.server.submit(hj)
        self.server.expect(JOB, {'job_state': 'R'}, id=hjid)
        sp_jid = self.server.filter(JOB, {'job_state': 'S'}, extend='t')
        susp_job_id = sp_jid['job_state=S'][0]
        now2 = time.time()
        self.server.expect(JOB, {'accrue_type': RUN_TIME}, id=hjid)
        self.logger.info('Waiting for eligible_time to accrue')
        time.sleep(WAIT_TIME)
        self.server.expect(JOB, {'accrue_type': ELIGIBLE_TIME}, id=susp_job_id)
        ndiff = int(now2 - now1)
        self.check_eligible_time(start=val1 + WAIT_TIME + ndiff,
                                 jid=susp_job_id, offset=10)
        now = time.time()
        self.server.holdjob(hjid, USER_HOLD)
        self.server.rerunjob(hjid)
        self.server.expect(JOB, {'job_state': 'H'}, id=hjid)
        self.logger.info('Waiting for eligible_time to accrue')
        time.sleep(WAIT_TIME)
        self.server.expect(JOB, {'accrue_type': INELIGIBLE_TIME}, id=hjid)
        self.server.rlsjob(hjid, USER_HOLD)
        self.server.expect(JOB, {'job_state': 'R'}, id=hjid)
        self.server.delete(susp_job_id, extend='force')
        self.server.expect(JOB, {'job_state': 'X'}, id=susp_job_id)
        self.logger.info('Job accrues eligible_time to exit_time')
        v_msg = susp_job_id + ";Accrue type has changed to exiting, "
        v_msg += "previous accrue type was eligible_time"
        self.server.log_match(v_msg, max_attempts=20, starttime=now)

    def test_job_array_ineligible_time(self):
        """
        Test that Job array accrues ineligible_time, if all subjobs are
        instantiated
        """
        WTIME1 = 60
        ja1 = Job(attrs={ATTR_J: '1-13', ATTR_l + '.walltime': WTIME1})
        ja1.create_script(self.pscript)
        ja_id1 = self.server.submit(ja1)
        now1 = time.time()
        self.server.expect(JOB, {'job_state': 'B'}, id=ja_id1)
        self.server.expect(JOB, {'job_state=R': 12}, count=True,
                           id=ja_id1, extend='t')
        subjid = ja1.create_subjob_id(ja_id1, 13)
        self.server.expect(JOB, {'job_state': 'Q'}, id=subjid)
        self.logger.info('Waiting for eligible_time to accrue')
        now2 = time.time()
        time.sleep(WAIT_TIME)
        ndiff = int(now2 - now1)
        self.check_eligible_time(start=WAIT_TIME + ndiff, jid=ja_id1)
        self.server.expect(JOB, {'job_state=X': 12}, count=True,
                           id=ja_id1, extend='t', offset=WTIME1)
        self.server.expect(JOB, {'job_state': 'R'}, id=subjid)
        self.check_eligible_time(start=WTIME1, jid=ja_id1, offset=20)
        self.server.expect(JOB, {'accrue_type': INELIGIBLE_TIME}, ja_id1)
        self.logger.info('job accrue eligible_time to ineligible_time')
        self.log_check(jid=ja_id1, from_acrtype='eligible_time',
                       to_acrtype='ineligible_time', from_jid=ja_id1,
                       to_jid=subjid)

    def test_accrue_type_subjob_rerun(self):
        """
        Test that if the subjob is rerun, the behaviour is same as it is being
        run for the first time
        """
        WTIME = 60
        now1 = time.time()
        ja1 = Job(attrs={ATTR_J: '1-13', ATTR_l + '.walltime': WTIME})
        ja_id = self.server.submit(ja1)
        self.server.expect(JOB, {'job_state=R': 12}, count=True,
                           id=ja_id, extend='t')
        self.logger.info('Waiting till walltime of submitted job array')
        self.server.expect(JOB, {'job_state=X': 12}, count=True,
                           id=ja_id, extend='t', offset=WTIME)
        sjid13 = ja1.create_subjob_id(ja_id, 13)
        self.server.expect(JOB, {'job_state': 'R'}, sjid13, extend='t')
        self.logger.info('subjob accrue type changes to run_time')
        self.log_check(jid=sjid13, from_acrtype='eligible_time',
                       to_acrtype='run_time', from_jid=ja_id, to_jid=sjid13,
                       vtime=now1)
        self.logger.info('Waiting for few seconds of run time of subjob 13')
        time.sleep(WAIT_TIME)
        now2 = time.time()
        self.server.rerunjob(jobid=sjid13, extend='force')
        self.server.expect(JOB, {'job_state': 'R'}, sjid13, extend='t')
        self.logger.info('subjob accrue_type changes to run_time')
        v_msg = sjid13 + ";Accrue type has changed to run_time, "
        v_msg += "previous accrue type was eligible_time"
        self.server.log_match(v_msg, max_attempts=20, starttime=now2)

    def test_subjob_borrow_eligible_time_from_parent(self):
        """
        subjob will borrow the eligible_time value from parent job after
        it is instantiated
        """
        WTIME = 60
        a = {'Resource_List.select': '12:ncpus=1'}
        j1 = Job(attrs=a)
        j1.set_sleep_time(WTIME)
        j_id1 = self.server.submit(j1)
        self.server.expect(JOB, {'job_state': 'R'}, j_id1)
        ja2 = Job(attrs={ATTR_J: '1-14'})
        ja2.create_script(self.pscript)
        ja_id2 = self.server.submit(ja2)
        self.server.expect(JOB, {'job_state': 'Q'}, ja_id2)
        self.server.expect(JOB, {'job_state': 'F'}, id=j_id1,
                           extend='x', interval=5, offset=WTIME)
        now1 = time.time()
        self.server.expect(JOB, {'job_state': 'B'}, ja_id2)
        subjid1 = ja2.create_subjob_id(ja_id2, 1)
        now2 = time.time()
        ndiff1 = int(now2 - now1)
        self.check_eligible_time(start=WTIME + ndiff1, jid=ja_id2, offset=15)
        # Reduced the start range of time for first subjob since its state
        # changes first and time would be less comparted to others
        self.check_eligible_time(start=WTIME + ndiff1 - 5, jid=subjid1,
                                 offset=10)
        subjid13 = ja2.create_subjob_id(ja_id2, 13)
        self.server.expect(JOB, {"eligible_time": "00:00:00"}, op=EQ,
                           id=subjid13)
        self.server.expect(JOB, {'job_state': 'R'}, subjid1, offset=5)
        self.server.delete(subjid1, extend='force')
        self.log_check(jid=subjid1, from_acrtype='run_time',
                       to_acrtype='exit_time')
        self.server.expect(JOB, {'job_state': 'R'}, subjid13)
        self.logger.info('job accrues eligible_time to run_time')
        self.log_check(jid=subjid13, from_acrtype='eligible_time',
                       to_acrtype='run_time', from_jid=ja_id2)
        self.server.expect(JOB, {'accrue_type': ELIGIBLE_TIME}, id=ja_id2)
        now3 = time.time()
        ndiff2 = int(now3 - now2)
        # Increased the offset to 20 since for job array
        self.check_eligible_time(start=WTIME + ndiff1 + ndiff2, jid=ja_id2)
        # Reduced the start range of time for first subjob since its state
        # changes first and time would be less comparted to others
        self.check_eligible_time(start=WTIME + ndiff1 + ndiff2 - 5,
                                 jid=subjid13, offset=10)

    def test_job_array_hold_accrual(self):
        """
        test for job array accrual of ineligible time for user hold and
        eligible time for system hold
        """
        for hld in [USER_HOLD, SYSTEM_HOLD]:
            a = {ATTR_l + '.select': '1:ncpus=' + self.num_ncpus,
                 ATTR_k: 'oe'}
            J1 = Job(attrs=a)
            J1.create_script(self.pscript)
            j_id = self.server.submit(J1)
            self.server.expect(JOB, {'job_state': 'R'}, j_id)
            ja1 = Job(attrs={ATTR_J: '1-100'})
            ja1.create_script(self.pscript)
            ja_id1 = self.server.submit(ja1)
            self.server.expect(JOB, {'job_state': 'Q'}, id=ja_id1)
            self.logger.info('Waiting for eligible_time to accrue')
            time.sleep(WAIT_TIME)
            val1 = self.check_eligible_time(start=WAIT_TIME, jid=ja_id1,
                                            offset=10)
            now1 = time.time()
            self.server.delete(j_id, extend='force')
            self.server.expect(JOB, {'job_state': 'B'}, id=ja_id1)
            self.logger.info('Waiting for eligible_time to accrue')
            time.sleep(WAIT_TIME)
            now2 = time.time()
            ndiff1 = int(now2 - now1)
            val2 = self.check_eligible_time(start=val1 + ndiff1,
                                            jid=ja_id1, offset=10)
            now3 = time.time()
            self.server.holdjob(ja_id1, hld)
            self.server.expect(JOB, {'job_state': 'H'}, id=ja_id1)
            now4 = time.time()
            ndiff2 = int(now4 - now3)
            # Reduced 5 seconds for start value since after hold, eligible_time
            # is not accrued
            stime = ndiff2 + val2 - 5
            if hld == SYSTEM_HOLD:
                stime = ndiff2 + val2
            val3 = self.check_eligible_time(start=stime, jid=ja_id1,
                                            offset=10)
            msg = 'Waiting to check if eligible_time will accrue or not'
            self.logger.info(msg)
            time.sleep(WAIT_TIME)
            stime = val3
            if hld == SYSTEM_HOLD:
                stime = val3 + WAIT_TIME
            val4 = self.check_eligible_time(start=stime, jid=ja_id1, offset=10)
            now5 = time.time()
            self.server.rlsjob(ja_id1, hld)
            now6 = time.time()
            stime = val4
            if hld == SYSTEM_HOLD:
                stime = val4 + int(now6 - now5)
            val5 = self.check_eligible_time(start=stime, jid=ja_id1, offset=10)
            now7 = time.time()
            self.server.expect(JOB, {'job_state': 'B'}, id=ja_id1)
            self.server.manager(MGR_CMD_SET, SERVER, {'scheduling': 'True'})
            now8 = time.time()
            self.logger.info('Waiting for eligible_time to accrue')
            time.sleep(WAIT_TIME)
            ndiff2 = int(now8 - now7)
            val6 = self.check_eligible_time(start=val5 + ndiff2 + WAIT_TIME,
                                            jid=ja_id1, offset=10)
            now9 = time.time()
            self.server.rerunjob(jobid=ja_id1)
            self.server.expect(JOB, {'job_state': 'B'}, id=ja_id1)
            now10 = time.time()
            ndiff3 = int(now10 - now9)
            self.check_eligible_time(start=val6 + ndiff3, jid=ja_id1,
                                     offset=10)
            self.server.cleanup_jobs()

    def test_eligible_time_and_starving(self):
        """
        Test to verify the behavior when job crosses maximum starving time
        while accruing eligible_time and ineligible_time
        """
        STARVE_TIME = 10
        self.server.manager(MGR_CMD_SET, NODE,
                            {'resources_available.ncpus': '12'},
                            self.mom.shortname)
        a = {'max_run': '[u:PBS_GENERIC=2]'}
        self.server.manager(MGR_CMD_SET, SERVER, a)
        a = {'help_starving_jobs': "True   ALL",
             'max_starve': '00:00:' + str(STARVE_TIME)}
        self.scheduler.set_sched_config(a)
        self.server.manager(MGR_CMD_SET, SCHED, {'log_events': 2047})
        jid_list = []
        a = {ATTR_l + '.select': '1:ncpus=1', ATTR_k: 'oe'}
        J1 = Job(attrs=a)
        J1.set_sleep_time(500)
        jid_list.append(self.server.submit(J1))
        self.server.expect(JOB, {ATTR_state: 'R'}, id=jid_list[0])
        J2 = Job(attrs=a)
        J2.set_sleep_time(500)
        jid_list.append(self.server.submit(J2))
        self.server.expect(JOB, {ATTR_state: 'R'}, id=jid_list[1])
        a = {ATTR_l + '.select': '10:ncpus=1', ATTR_k: 'oe'}
        J3 = Job(attrs=a)
        J3.set_sleep_time(500)
        jid_list.append(self.server.submit(J3))
        self.server.expect(JOB, {ATTR_state: 'Q'}, id=jid_list[2])
        self.logger.info("Waiting for starving time")
        time.sleep(STARVE_TIME + 5)
        self.check_eligible_time(start=0, jid=jid_list[2])
        self.server.expect(JOB, {'accrue_type': INELIGIBLE_TIME},
                           id=jid_list[2])
        a = {'scheduling': 'True'}
        self.server.manager(MGR_CMD_SET, SERVER, a)
        log_msg = '%s;Job is starving' % jid_list[2]
        self.scheduler.log_match(msg=log_msg, max_attempts=30, existence=False)
        a = {ATTR_l + '.select': '10:ncpus=1', ATTR_k: 'oe'}
        J4 = Job(TEST_USER1, attrs=a)
        J4.set_sleep_time(500)
        jid_list.append(self.server.submit(J4))
        self.server.expect(JOB, {ATTR_state: 'R'}, id=jid_list[3])
        self.server.deljob(jid_list[0])
        a = {'scheduling': 'True'}
        self.server.manager(MGR_CMD_SET, SERVER, a)
        self.server.expect(JOB, {ATTR_state: 'Q'}, id=jid_list[2])
        self.server.expect(JOB, {'accrue_type': ELIGIBLE_TIME}, id=jid_list[2])
        self.logger.info("Waiting for starving time")
        time.sleep(STARVE_TIME + 5)
        a = {'scheduling': 'True'}
        self.server.manager(MGR_CMD_SET, SERVER, a)
        log_msg = '%s;Job is starving' % jid_list[2]
        self.scheduler.log_match(msg=log_msg, max_attempts=30)

    def test_eligible_time_disabled(self):
        """
        Test to verify the behavior when eligible_time feature is disabled
        or unset
        """
        now = time.time()
        jlist = []
        for i in range(2):
            jlist = self.common_submission(queued=2)
            self.check_eligible_time(start=WAIT_TIME, jid=jlist[1])
            self.server.expect(JOB, {'accrue_type': ELIGIBLE_TIME},
                               id=jlist[1])
            if i == 0:
                a = {'eligible_time_enable': 0}
                self.server.manager(MGR_CMD_SET, SERVER, a)
            else:
                self.server.manager(MGR_CMD_UNSET, SERVER,
                                    'eligible_time_enable')
            self.logger.info('Waiting to allow eligible_time to accrue')
            time.sleep(WAIT_TIME)
            qstat = self.server.status(JOB, id=jlist[1])
            self.assertNotIn('eligible_time', qstat[0])
            self.assertNotIn('accrue_type', qstat[0])
            msg = 'Cannot set attribute when eligible_time_enable is OFF'
            with self.assertRaises(PbsAlterError, msg=msg) as e:
                self.server.alterjob(jlist[1],
                                     {ATTR_W: 'eligible_time=00:03:00'})
            self.assertIn(msg, e.exception.msg[0])
            qstat = self.server.status(JOB, id=jlist[1])
            self.assertNotIn('eligible_time', qstat[0])
            self.assertNotIn('accrue_type', qstat[0])
            self.server.holdjob(jlist[2], USER_HOLD)
            self.server.alterjob(jlist[1], {'Resource_List.walltime': 10})
            self.server.deljob(jlist[0])
            self.server.expect(JOB, {ATTR_state: 'R'}, id=jlist[1], offset=4)
            self.server.log_match("%s;Exit_status=" % jlist[1], interval=4)
            record = "E;%s;" % jlist[1]
            line = self.server.accounting_match(msg=record, id=jlist[1],
                                                n='ALL',
                                                max_attempts=10,
                                                starttime=now)
            self.assertNotIn('eligible_time', line)
            a = {'eligible_time_enable': 1}
            self.server.manager(MGR_CMD_SET, SERVER, a)
            self.server.rlsjob(jlist[2], USER_HOLD)
            qstat = self.server.status(JOB, id=jlist[2])
            self.assertIn('eligible_time', qstat[0])
            self.server.expect(JOB, {'accrue_type': RUN_TIME}, id=jlist[2])
            self.server.cleanup_jobs()

    def test_ineligible_time_of_job_above_limit(self):
        """
        Job accrues ineligible time after exceeding the max_run
        limit at server and queue level
        """
        level = [SERVER, QUEUE]
        lim_list = ["[u:PBS_GENERIC=2]", "[g:PBS_GENERIC=2]"]
        limit_type = ['max_run', 'max_run_res.ncpus']
        for ltype in limit_type:
            for lim in lim_list:
                for l in level:
                    a = {ltype: lim}
                    j_id = []
                    if l == QUEUE:
                        self.server.manager(MGR_CMD_SET, QUEUE, a, id='workq')
                    elif l == SERVER:
                        self.server.manager(MGR_CMD_SET, SERVER, a)
                    for _ in range(3):
                        a = {'Resource_List.select': '1:ncpus=1',
                             ATTR_l + '.walltime': 100, 'group_list': TSTGRP0}
                        j1 = Job(TEST_USER1, a)
                        j1.create_script(self.pscript)
                        j_id.append(self.server.submit(j1))
                    self.server.expect(JOB, {'job_state': 'R'}, j_id[0])
                    self.server.expect(JOB, {'job_state': 'R'}, j_id[1])
                    self.server.expect(JOB, {'job_state': 'Q'}, j_id[2])
                    self.server.expect(JOB, {'accrue_type': INELIGIBLE_TIME},
                                       id=j_id[2])
                    self.server.delete(j_id)

    @skip("PBS-26548")
    def test_user_and_system_held_job_ineligible_time(self):
        """
        Test that running & queued job accrue ineligible time when held
        with user and system hold one over the other
        """
        for i in [0, 1]:
            a = {ATTR_l + '.select': '1:ncpus=' + self.num_ncpus,
                 ATTR_k: 'oe'}
            J1 = Job(attrs=a)
            J1.set_sleep_time(500)
            jid = self.server.submit(J1)
            self.server.expect(JOB, {ATTR_state: 'R'}, id=jid)
            if i == 0:
                j = jid
            else:
                a = {ATTR_l + '.select': '1:ncpus=1', ATTR_k: 'oe'}
                J2 = Job(attrs=a)
                J2.set_sleep_time(500)
                jid2 = self.server.submit(J2)
                self.server.expect(JOB, {ATTR_state: 'Q'}, id=jid2)
                j = jid2
            val = self.check_eligible_time(start=0, jid=j, offset=10)
            self.server.holdjob(j, USER_HOLD)
            self.server.rerunjob(jobid=j)
            self.server.expect(JOB, {'job_state': 'H',
                               'accrue_type': INELIGIBLE_TIME}, id=j)
            self.logger.info('Waiting to allow eligible_time to accrue')
            time.sleep(WAIT_TIME)
            self.server.expect(JOB, {'accrue_type': INELIGIBLE_TIME}, id=j)
            self.check_eligible_time(start=val, jid=j, offset=5)
            self.server.holdjob(j, SYSTEM_HOLD)
            self.server.expect(JOB, {'job_state': 'H',
                               'accrue_type': INELIGIBLE_TIME}, id=j)
            self.server.rlsjob(j, holdtype=SYSTEM_HOLD)
            self.server.expect(JOB, {'accrue_type': INELIGIBLE_TIME}, id=j)
            self.logger.info('Waiting for eligible_time to accrue')
            time.sleep(WAIT_TIME)
            self.check_eligible_time(start=val, jid=j, offset=5)
            self.server.expect(JOB, {'accrue_type': INELIGIBLE_TIME}, id=j)
            self.server.rlsjob(j, holdtype=USER_HOLD)
            a = {'job_state': 'R', 'accrue_type': RUN_TIME}
            if i == 1:
                a = {'job_state': 'Q', 'accrue_type': ELIGIBLE_TIME}
            self.server.expect(JOB, a, id=j)
            self.server.cleanup_jobs()

    def test_hold_types_job_ineligible_time(self):
        """
        Test that job accrues eligible time when held
        with hold types system, operator & bad password
        """
        a = {ATTR_l + '.select': '1:ncpus=' + self.num_ncpus,
             ATTR_k: 'oe'}
        J1 = Job(attrs=a)
        J1.set_sleep_time(500)
        a = {ATTR_l + '.select': '1:ncpus=1', ATTR_k: 'oe'}
        J2 = Job(attrs=a)
        J2.set_sleep_time(500)
        jid = self.server.submit(J1)
        self.server.expect(JOB, {ATTR_state: 'R'}, id=jid)
        jid2 = self.server.submit(J2)
        self.server.expect(JOB, {ATTR_state: 'Q'}, id=jid2)
        self.logger.info('Waiting to allow eligible_time to accrue')
        time.sleep(WAIT_TIME)
        val = self.check_eligible_time(start=WAIT_TIME, jid=jid2)
        self.server.deljob(jid)
        self.server.expect(JOB, {ATTR_state: 'R'}, id=jid2)
        val3 = val
        for hld in [SYSTEM_HOLD, OTHER_HOLD, BAD_PASSWORD_HOLD]:
            val1 = val3
            now1 = time.time()
            self.server.holdjob(jid2, hld, runas='root')
            time.sleep(2)
            self.server.rerunjob(jid2)
            self.server.expect(JOB, {'job_state': 'H',
                               'accrue_type': ELIGIBLE_TIME}, id=jid2)
            self.logger.info('Waiting to allow eligible_time to accrue')
            time.sleep(WAIT_TIME)
            now2 = time.time()
            ndiff = int(now2 - now1)
            val2 = self.check_eligible_time(start=val1 + ndiff, jid=jid2)
            now3 = time.time()
            self.server.rlsjob(jid2, holdtype=hld, runas='root')
            now4 = time.time()
            ndiff2 = int(now4 - now3)
            val3 = self.check_eligible_time(start=val2 + ndiff2, jid=jid2)

    def test_eligible_time_in_job_sort_formula(self):
        """
        Test to verify the behavior of eligible_time in job_sort_formula,
        job is under formula threshold and when eligible time is disabled
        """
        a = {'type': 'float'}
        self.server.manager(MGR_CMD_CREATE, RSC, a, id='A')
        self.server.manager(MGR_CMD_SET, SCHED, {'log_events': 2047})
        a = {'resources_default.A': 0.1}
        self.server.manager(MGR_CMD_SET, SERVER, a)
        a = {'job_sort_formula': '20 + A * eligible_time'}
        self.server.manager(MGR_CMD_SET, SERVER, a)
        jlist = []
        jlist = self.common_submission()
        val1 = self.check_eligible_time(start=0, jid=jlist[0])
        val2 = self.check_eligible_time(start=WAIT_TIME, jid=jlist[1])
        f1 = 20 + (0.1 * val1)
        f2 = 20 + (0.1 * val2)
        now = time.time()
        self.server.manager(MGR_CMD_SET, SERVER, {'scheduling': 'True'})
        fe_msg = ";Formula Evaluation = "
        m = self.scheduler.log_match(jlist[0] + fe_msg + str(f1),
                                     starttime=now)
        n = self.scheduler.log_match(jlist[1] + fe_msg + str(int(f2)),
                                     starttime=now)
        a = {'job_sort_formula_threshold': '40'}
        self.server.manager(MGR_CMD_SET, SCHED, a)
        val2 = self.check_eligible_time(start=val2, jid=jlist[1])
        self.server.expect(JOB, {'accrue_type': INELIGIBLE_TIME}, id=jlist[1])
        a = {'eligible_time_enable': False}
        self.server.manager(MGR_CMD_SET, SERVER, a)
        self.logger.info('Waiting for eligible_time to accrue')
        time.sleep(WAIT_TIME)
        self.server.manager(MGR_CMD_SET, SERVER, {'scheduling': 'True'})
        m = self.scheduler.log_match(jlist[0] + ";Formula Evaluation = 20",
                                     starttime=now)
        n = self.scheduler.log_match(jlist[1] + ";Formula Evaluation = 20",
                                     starttime=now)

    def test_eligible_time_qrun_job(self):
        """
        Test to verify the eligible_time of qrun job
        """
        self.server.manager(MGR_CMD_SET, SERVER, {'scheduling': 'False'})
        jid_list = []

        for _ in range(3):
            a = {ATTR_l + '.select': '1:ncpus=1', ATTR_k: 'oe'}
            J1 = Job(attrs=a)
            J1.set_sleep_time(500)
            jid_list.append(self.server.submit(J1))
        for i in range(3):
            self.server.expect(JOB, {'accrue_type': ELIGIBLE_TIME},
                               id=jid_list[i])
        self.server.runjob(jobid=jid_list[2])
        self.server.expect(JOB, {'accrue_type': RUN_TIME}, id=jid_list[2])
        for i in range(2):
            self.server.expect(JOB, {'accrue_type': ELIGIBLE_TIME},
                               id=jid_list[i])

    def test_eligible_time_qrun_held_job(self):
        """
        Test to verify the eligible_time when 'qrun' done on held job
        """
        self.server.manager(MGR_CMD_SET, SERVER, {'scheduling': 'False'})
        for i in [0, 1]:
            a = {ATTR_l + '.select': '1:ncpus=1', ATTR_k: 'oe'}
            J = Job(attrs=a)
            J.set_sleep_time(500)
            if i == 1:
                J. set_attributes({ATTR_h: None})
                acr_type = 1
                wt_time = 0
            else:
                acr_type = 2
                wt_time = WAIT_TIME
            jid = self.server.submit(J)
            self.server.expect(JOB, {'accrue_type': acr_type}, id=jid)
            self.server.holdjob(jid, SYSTEM_HOLD)
            self.logger.info('Waiting for eligible_time to accrue')
            time.sleep(WAIT_TIME)
            val = self.check_eligible_time(start=wt_time, jid=jid)
            self.server.runjob(jobid=jid, location=self.mom.shortname)
            self.server.expect(JOB, {'accrue_type': RUN_TIME}, id=jid)
            self.check_eligible_time(start=val, jid=jid)
            self.server.deljob(jid)

    def test_eligible_time_when_no_license(self):
        """
        Test to verify the eligible_time when no licenses are available
        """
        res_val = self.server.status(SERVER)
        self.lic_info = res_val[0]['pbs_license_info']
        self.server.manager(MGR_CMD_UNSET, SERVER, 'pbs_license_info')
        jid_list = []
        for _ in range(3):
            a = {ATTR_l + '.select': '1:ncpus=1', ATTR_k: 'oe'}
            J1 = Job(attrs=a)
            J1.set_sleep_time(500)
            jid_list.append(self.server.submit(J1))
        for i in range(3):
            self.server.expect(JOB, {'job_state': 'Q',
                               'accrue_type': ELIGIBLE_TIME}, id=jid_list[i])
        self.logger.info('Waiting for eligible_time to accrue')
        time.sleep(WAIT_TIME)
        for i in range(3):
            self.check_eligible_time(start=WAIT_TIME, jid=jid_list[i])

    def test_accrue_type_checkpoint_user_hold(self):
        """
        Test to verify the eligible_time when no licenses are available
        """
        abort_script = """#!/bin/bash
kill -9 $PBS_SID
"""
        self.mom.add_checkpoint_abort_script(
                              body=abort_script)
        restart_script = """#!/bin/bash
sleep 100
"""
        self.mom.add_restart_script(
                              body=restart_script)
        self.mom.add_config({'$restart_transmogrify': 'true'})
        self.server.manager(MGR_CMD_SET, SCHED, {'preempt_order': 'C'})
        a = {ATTR_l + '.select': '1:ncpus=1', ATTR_k: 'oe'}
        J1 = Job(attrs=a)
        J1.create_script(self.pscript)
        jid = self.server.submit(J1)
        self.server.expect(JOB, {'job_state': 'R'}, id=jid)
        self.server.holdjob(jid, USER_HOLD)
        self.logger.info('Waiting for eligible_time to accrue')
        time.sleep(WAIT_TIME)
        self.check_eligible_time(start=0, jid=jid)
        self.server.expect(JOB, {'job_state': 'H',
                           'accrue_type': INELIGIBLE_TIME}, id=jid)
        self.server.rlsjob(jid, USER_HOLD)
        self.server.expect(JOB, {'job_state': 'R'}, id=jid)

    def test_accrue_type_node_offline(self):
        """
        Test to verify the eligible_time when node is offline
        """
        self.server.manager(MGR_CMD_SET, NODE, {'state': 'offline'},
                            id=self.mom.shortname)
        a = {ATTR_l + '.select': '1:ncpus=1', ATTR_k: 'oe'}
        J1 = Job(attrs=a)
        J1.create_script(self.pscript)
        jid = self.server.submit(J1)
        attrib = {'job_state': 'Q', 'comment':
                  "Not Running: Not enough free nodes available",
                  'accrue_type': ELIGIBLE_TIME}
        self.server.expect(JOB, attrib, id=jid, attrop=PTL_AND)
        pbs_exec = self.server.pbs_conf['PBS_EXEC']
        self.server.manager(MGR_CMD_SET, NODE, {'state': (DECR, 'offline')},
                            id=self.mom.shortname)
        self.server.expect(JOB, {'job_state': 'R', 'accrue_type': RUN_TIME},
                           id=jid)

    def tearDown(self):
        if self.lic_info:
            a = {'pbs_license_info': self.lic_info}
            self.server.manager(MGR_CMD_SET, SERVER, a)
            self.lic_info = None
        a = {'job_history_enable': 'False'}
        self.server.manager(MGR_CMD_SET, SERVER, a)
