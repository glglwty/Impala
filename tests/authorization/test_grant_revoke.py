# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Client tests for SQL statement authorization
# In these tests we run all the tests twice. Once with just using cache, hence the
# long sentry poll, and once by ensuring refreshes happen from Sentry.

import grp
import pytest
import os
import sys
import subprocess
from getpass import getuser
from os import getenv
from time import sleep

from tests.common.test_dimensions import create_uncompressed_text_dimension
from tests.common.sentry_cache_test_suite import SentryCacheTestSuite, TestObject
from tests.util.calculation_util import get_random_id
from tests.verifiers.metric_verifier import MetricVerifier

SENTRY_CONFIG_FILE = getenv('IMPALA_HOME') + \
    '/fe/src/test/resources/sentry-site.xml'

# The polling frequency used by catalogd to refresh Sentry privileges.
# The long polling is so we can check updates just for the cache.
# The other polling is so we can get the updates without having to wait.
SENTRY_POLLING_FREQUENCY_S = 1
SENTRY_LONG_POLLING_FREQUENCY_S = 120
# The timeout, in seconds, when waiting for a refresh of Sentry privileges.
# This is also used as a delay before executing a statement. The difference between
# the two usages is one is used to check results and once successful can return before
# the time is up.  The other is to enforce a delay with no short circuit.
SENTRY_REFRESH_TIMEOUT_S = SENTRY_POLLING_FREQUENCY_S * 3


class TestGrantRevoke(SentryCacheTestSuite):
  @classmethod
  def add_test_dimensions(cls):
    super(TestGrantRevoke, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(
        create_uncompressed_text_dimension(cls.get_workload()))

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  def setup_method(self, method):
    super(TestGrantRevoke, self).setup_method(method)
    self.__test_cleanup()

  def teardown_method(self, method):
    self.__test_cleanup()
    super(TestGrantRevoke, self).teardown_method(method)

  def __test_cleanup(self):
    # Clean up any old roles created by this test
    for role_name in self.client.execute("show roles").data:
      if 'grant_revoke_test' in role_name:
        self.client.execute("drop role %s" % role_name)

    # Cleanup any other roles that were granted to this user.
    # TODO: Update Sentry Service config and authorization tests to use LocalGroupMapping
    # for resolving users -> groups. This way we can specify custom test users that don't
    # actually exist in the system.
    group_name = grp.getgrnam(getuser()).gr_name
    for role_name in self.client.execute("show role grant group `%s`" % group_name).data:
      self.client.execute("drop role %s" % role_name)

    # Create a temporary admin user so we can actually view/clean up the test
    # db.
    try:
      self.client.execute("create role grant_revoke_test_admin")
      self.client.execute("grant all on server to grant_revoke_test_admin")
      self.client.execute("grant role grant_revoke_test_admin to group `%s`" % group_name)
      self.cleanup_db('grant_rev_db', sync_ddl=0)
    finally:
      self.client.execute("drop role grant_revoke_test_admin")

  @classmethod
  def restart_first_impalad(cls):
    impalad = cls.cluster.impalads[0]
    impalad.restart()
    cls.client = impalad.service.create_beeswax_client()

  @pytest.mark.execute_serially
  @SentryCacheTestSuite.with_args(
      impalad_args="--server_name=server1",
      catalogd_args="--sentry_config=" + SENTRY_CONFIG_FILE,
      sentry_config=SENTRY_CONFIG_FILE)
  def test_grant_revoke(self, vector):
    self.run_test_case('QueryTest/grant_revoke', vector, use_db="default")

  @pytest.mark.execute_serially
  @SentryCacheTestSuite.with_args(
      impalad_args="--server_name=server1",
      catalogd_args="--sentry_config=" + SENTRY_CONFIG_FILE,
      sentry_config=SENTRY_CONFIG_FILE)
  def test_grant_revoke_kudu(self, vector):
    if getenv("KUDU_IS_SUPPORTED") == "false":
      pytest.skip("Kudu is not supported")
    self.run_test_case('QueryTest/grant_revoke_kudu', vector, use_db="default")

  @pytest.mark.execute_serially
  @SentryCacheTestSuite.with_args(
      impalad_args="--server_name=server1",
      catalogd_args="--sentry_config=" + SENTRY_CONFIG_FILE +
      " --sentry_catalog_polling_frequency_s=" + str(SENTRY_POLLING_FREQUENCY_S),
      sentry_config=SENTRY_CONFIG_FILE)
  def test_grant_revoke_with_sentry(self, vector):
    self.__execute_with_grant_option_tests(TestObject(TestObject.SERVER), "all",
        sentry_refresh_timeout_s=SENTRY_REFRESH_TIMEOUT_S)
    self.__execute_with_grant_option_tests(TestObject(TestObject.DATABASE,
        "grant_rev_db"), "all", sentry_refresh_timeout_s=SENTRY_REFRESH_TIMEOUT_S)
    self.__execute_with_grant_option_tests(TestObject(TestObject.TABLE,
        "grant_rev_db.tbl1"), "all", sentry_refresh_timeout_s=SENTRY_REFRESH_TIMEOUT_S)
    self.__execute_with_grant_option_tests(TestObject(TestObject.VIEW,
        "grant_rev_db.tbl1"), "all", sentry_refresh_timeout_s=SENTRY_REFRESH_TIMEOUT_S)
    self.__execute_with_grant_option_tests(TestObject(TestObject.SERVER), "select",
        sentry_refresh_timeout_s=SENTRY_REFRESH_TIMEOUT_S)
    self.__execute_with_grant_option_tests(TestObject(TestObject.DATABASE,
        "grant_rev_db"), "select", sentry_refresh_timeout_s=SENTRY_REFRESH_TIMEOUT_S)
    self.__execute_with_grant_option_tests(TestObject(TestObject.TABLE,
        "grant_rev_db.tbl1"), "select", sentry_refresh_timeout_s=SENTRY_REFRESH_TIMEOUT_S)
    self.__execute_with_grant_option_tests(TestObject(TestObject.VIEW,
        "grant_rev_db.tbl1"), "select", sentry_refresh_timeout_s=SENTRY_REFRESH_TIMEOUT_S)

  @pytest.mark.execute_serially
  @SentryCacheTestSuite.with_args(
      impalad_args="--server_name=server1",
      catalogd_args="--sentry_config=" + SENTRY_CONFIG_FILE +
      " --sentry_catalog_polling_frequency_s=" + str(SENTRY_LONG_POLLING_FREQUENCY_S),
      sentry_config=SENTRY_CONFIG_FILE)
  def test_grant_revoke_with_sentry_long_poll(self, vector):
    self.__execute_with_grant_option_tests(TestObject(TestObject.SERVER), "all")
    self.__execute_with_grant_option_tests(TestObject(TestObject.DATABASE,
        "grant_rev_db"), "all")
    self.__execute_with_grant_option_tests(TestObject(TestObject.TABLE,
        "grant_rev_db.tbl1"), "all")
    self.__execute_with_grant_option_tests(TestObject(TestObject.VIEW,
        "grant_rev_db.tbl1"), "all")
    self.__execute_with_grant_option_tests(TestObject(TestObject.SERVER), "select")
    self.__execute_with_grant_option_tests(TestObject(TestObject.DATABASE,
        "grant_rev_db"), "select")
    self.__execute_with_grant_option_tests(TestObject(TestObject.TABLE,
        "grant_rev_db.tbl1"), "select")
    self.__execute_with_grant_option_tests(TestObject(TestObject.VIEW,
        "grant_rev_db.tbl1"), "select")

  @pytest.mark.execute_serially
  @SentryCacheTestSuite.with_args(
    impalad_args="--server_name=server1",
    catalogd_args="--sentry_config=%s " % SENTRY_CONFIG_FILE +
                  "--sentry_catalog_polling_frequency_s=%d" % SENTRY_POLLING_FREQUENCY_S,
    sentry_config=SENTRY_CONFIG_FILE)
  def test_grant_revoke_with_sentry_refresh(self):
    group_name = grp.getgrnam(getuser()).gr_name
    role_name = "grant_revoke_test_sentry_refresh"
    try:
      self.client.execute("create role %s" % role_name)
      self.client.execute("grant role %s to group `%s`" % (role_name, group_name))
      # Add select and insert into the catalog.
      self.client.execute("grant select on table functional.alltypes to %s" % role_name)
      self.client.execute("grant insert on table functional.alltypes to %s" % role_name)
      # Grant alter privilege and revoke insert privilege using Sentry CLI.
      subprocess.check_call(
        ["/bin/bash", "-c", "%s/bin/sentryShell "
         "--conf %s/sentry-site.xml -gpr "
         "-p 'server=server1->db=functional->table=alltypes->action=alter' "
         "-r %s" % (os.getenv("SENTRY_HOME"), os.getenv("SENTRY_CONF_DIR"), role_name)],
        stdout=sys.stdout, stderr=sys.stderr)
      subprocess.check_call(
          ["/bin/bash", "-c", "%s/bin/sentryShell "
           "--conf %s/sentry-site.xml -rpr "
           "-p 'server=server1->db=functional->table=alltypes->action=insert' "
           "-r %s" % (os.getenv("SENTRY_HOME"), os.getenv("SENTRY_CONF_DIR"), role_name)],
          stdout=sys.stdout, stderr=sys.stderr)
      # Wait for the catalog update with Sentry refresh.
      sleep(SENTRY_POLLING_FREQUENCY_S + 3)
      # Ensure alter privilege was granted and insert privilege was revoked.
      result = self.execute_query_expect_success(self.client, "show grant role %s" %
                                                 role_name)
      assert len(result.data) == 2
      assert any('select' in x for x in result.data)
      assert any('alter' in x for x in result.data)
    finally:
      self.client.execute("drop role %s" % role_name)

  def __execute_with_grant_option_tests(self, test_obj, privilege,
      sentry_refresh_timeout_s=0):
    """
    Executes grant/revoke tests with grant option
    """
    # Do some initial setup only for this test.
    group_name = grp.getgrnam(getuser()).gr_name
    try:
      self.client.execute("create role grant_revoke_test_admin")
    except Exception:
      # Ignore this as it was already created on the last run.
      pass
    self.client.execute("grant all on server to grant_revoke_test_admin")
    self.client.execute("grant role grant_revoke_test_admin to group `%s`" % group_name)
    self.client.execute("create role grant_revoke_test_role")
    if test_obj.obj_type != TestObject.SERVER:
      self.user_query(self.client, "create %s if not exists %s %s %s"
          % (test_obj.obj_type, test_obj.obj_name, test_obj.table_def,
          test_obj.view_select), user="root")

    # Grant a basic privilege
    self.user_query(self.client, "grant %s on %s %s to role grant_revoke_test_role"
        % (privilege, test_obj.grant_name, test_obj.obj_name), user="root")

    # Ensure role has privilege.
    self.validate_privileges(self.client, "show grant role grant_revoke_test_role",
        test_obj, timeout_sec=sentry_refresh_timeout_s)

    # Try with grant option on existing privilege.
    test_obj.grant = True
    self.user_query(self.client, "grant %s on %s %s " % (privilege, test_obj.grant_name,
        test_obj.obj_name) + " to role grant_revoke_test_role with grant option",
        user="root")
    # Ensure role has updated privilege.
    self.validate_privileges(self.client, "show grant role grant_revoke_test_role",
        test_obj, timeout_sec=sentry_refresh_timeout_s)

    # Revoke the grant option
    self.user_query(self.client,
        "revoke grant option for %s on %s %s from role grant_revoke_test_role"
        % (privilege, test_obj.grant_name, test_obj.obj_name))
    # Ensure role has updated privilege.
    test_obj.grant = False
    self.validate_privileges(self.client, "show grant role grant_revoke_test_role",
        test_obj, delay_s=sentry_refresh_timeout_s)

    # Add the grant option back, then add a regular privilege
    self.user_query(self.client, "grant %s on %s %s to role grant_revoke_test_role"
        % (privilege, test_obj.grant_name, test_obj.obj_name) + " with grant option",
        user="root")
    self.user_query(self.client, "grant %s on %s %s to role grant_revoke_test_role"
        % (privilege, test_obj.grant_name, test_obj.obj_name), user="root")
    test_obj.grant = True
    self.validate_privileges(self.client, "show grant role grant_revoke_test_role",
        test_obj, timeout_sec=sentry_refresh_timeout_s)

    # Revoke the privilege
    self.user_query(self.client, "revoke %s on %s %s from role grant_revoke_test_role"
        % (privilege, test_obj.grant_name, test_obj.obj_name))
    result = self.user_query(self.client, "show grant role grant_revoke_test_role",
        delay_s=sentry_refresh_timeout_s)
    assert len(result.data) == 0

    # Reset the grant value
    test_obj.grant = False
    # Remove the role
    self.client.execute("drop role grant_revoke_test_role")

  @pytest.mark.execute_serially
  @SentryCacheTestSuite.with_args(
      impalad_args="--server_name=server1",
      catalogd_args="--sentry_config=" + SENTRY_CONFIG_FILE +
                    " --sentry_catalog_polling_frequency_s=1",
      sentry_config=SENTRY_CONFIG_FILE)
  def test_role_privilege_case(self, vector):
    """IMPALA-5582: Store sentry privileges in lower case. This
    test grants select privileges to roles assgined to tables/db
    specified in lower, upper and mix cases. This test verifies
    that these privileges do not vanish on a sentryProxy thread
    update.
    """
    db_name = "test_role_privilege_case_x_" + get_random_id(5)
    db_name_upper_case = "TEST_ROLE_PRIVILEGE_CASE_Y_" + get_random_id(5).upper()
    db_name_mixed_case = "TesT_Role_PRIVIlege_case_z" + get_random_id(5)
    role_name = "test_role_" + get_random_id(5)
    try:
      self.client.execute("create role {0}".format(role_name))
      self.client.execute("grant all on server to {0}".format(role_name))
      self.client.execute("grant role {0} to group `{1}`".format(role_name,
          grp.getgrnam(getuser()).gr_name))

      self.client.execute("create database " + db_name)
      self.client.execute("create database " + db_name_upper_case)
      self.client.execute("create database " + db_name_mixed_case)
      self.client.execute(
          "create table if not exists {0}.test1(i int)".format(db_name))
      self.client.execute("create table if not exists {0}.TEST2(i int)".format(db_name))
      self.client.execute("create table if not exists {0}.Test3(i int)".format(db_name))

      self.client.execute(
          "grant select on table {0}.test1 to {1}".format(db_name, role_name))
      self.client.execute(
          "grant select on table {0}.TEST2 to {1}".format(db_name, role_name))
      self.client.execute(
          "grant select on table {0}.TesT3 to {1}".format(db_name, role_name))
      self.client.execute("grant all on database {0} to {1}".format(db_name, role_name))
      self.client.execute(
          "grant all on database {0} to {1}".format(db_name_upper_case, role_name))
      self.client.execute(
          "grant all on database {0} to {1}".format(db_name_mixed_case, role_name))
      result = self.client.execute("show grant role {0}".format(role_name))
      assert any('test1' in x for x in result.data)
      assert any('test2' in x for x in result.data)
      assert any('test3' in x for x in result.data)
      assert any(db_name_upper_case.lower() in x for x in result.data)
      assert any(db_name_mixed_case.lower() in x for x in result.data)
      # Sleep for 2 seconds and make sure that the privileges
      # on all 3 tables still persist on a sentryProxy thread
      # update. sentry_catalog_polling_frequency_s is set to 1
      # seconds.
      sleep(2)
      result = self.client.execute("show grant role {0}".format(role_name))
      assert any('test1' in x for x in result.data)
      assert any('test2' in x for x in result.data)
      assert any('test3' in x for x in result.data)
      assert any(db_name_upper_case.lower() in x for x in result.data)
      assert any(db_name_mixed_case.lower() in x for x in result.data)
    finally:
      self.client.execute("drop database if exists {0}".format(db_name_upper_case))
      self.client.execute("drop database if exists {0}".format(db_name_mixed_case))
      self.client.execute("drop database if exists {0} cascade".format(db_name))
      self.client.execute("drop role {0}".format(role_name))

  @pytest.mark.execute_serially
  @SentryCacheTestSuite.with_args(
      impalad_args="--server_name=server1",
      catalogd_args="--sentry_config=" + SENTRY_CONFIG_FILE,
      sentry_config=SENTRY_CONFIG_FILE)
  def test_role_update(self, vector):
    """IMPALA-5355: The initial update from the statestore has the privileges and roles in
    reverse order if a role was modified, but not the associated privilege. Verify that
    Impala is able to handle this.
    """
    role_name = "test_role_" + get_random_id(5)
    try:
      self.client.execute("create role {0}".format(role_name))
      self.client.execute("grant all on server to {0}".format(role_name))
      # Wait a few seconds to make sure the update propagates to the statestore.
      sleep(3)
      # Update the role, increasing its catalog verion.
      self.client.execute("grant role {0} to group `{1}`".format(
          role_name, grp.getgrnam(getuser()).gr_name))
      result = self.client.execute("show tables in functional")
      assert 'alltypes' in result.data
      privileges_before = self.client.execute("show grant role {0}".format(role_name))
      # Wait a few seconds before restarting Impalad to make sure that the Catalog gets
      # updated.
      sleep(3)
      self.restart_first_impalad()
      verifier = MetricVerifier(self.cluster.impalads[0].service)
      verifier.wait_for_metric("catalog.ready", True)
      # Verify that we still have the right privileges after the first impalad was
      # restarted.
      result = self.client.execute("show tables in functional")
      assert 'alltypes' in result.data
      privileges_after = self.client.execute("show grant role {0}".format(role_name))
      assert privileges_before.data == privileges_after.data
    finally:
      self.client.execute("drop role {0}".format(role_name))
