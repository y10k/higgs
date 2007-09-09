# pure ruby transactional storage compatible with unix TAR format

require 'higgs/dbm'
require 'higgs/store'
require 'higgs/version'

# = pure ruby transactional storage compatible with unix TAR format
#
# Author:: $Author$
# Date:: $Date$
# Revision:: $Revision$
#
# == main classes
#
# [Higgs::Store] storage interface like pstore
# [Higgs::DBM] storage interface like dbm
# [Higgs::Utils::BackupManager] online backup utility (body of <tt>higgs_backup</tt>)
#
# == storage safety level
# === case of no backup
#
# [REQUIREMENTS] default
# [NORMAL SHUTDOWN] OK, no recovery
# [PROCESS ABORT] OK, recovery on read-write open
# [SYSTEM ABORT (OS abort)] NG, data is <em>NOT</em> consistent
#
# === case of online backup
#
# [REQUIREMENTS] open with parameters: <tt>jlog_rotate_max => 0</tt>,
#                <tt>jlog_rotate_service_uri => "druby://localhost:<em>appropriate_port_number</em>"</tt>
#                and execute <tt>higgs_backup</tt>
# [NORMAL SHUTDOWN] OK, no recovery
# [PROCESS ABORT] OK, automatic recovery on read-write open
# [SYSTEM ABORT (OS abort)] OK, restore from backup and recovery on read-write open
#
module Higgs
end

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
