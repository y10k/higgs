# backup manager
#
# Author:: $Author$
# Date:: $Date$
# Revision:: $Revision$
#

require 'drb'
require 'fileutils'
require 'higgs/storage'

module Higgs
  module Utils
    # = backup manager
    # == requirements for online-backup
    #
    # these parameters should be required at the open of Higgs::Storage.
    #
    # [<tt>:jlog_rotate_max</tt>] <tt>0</tt>. rotated journal logs shuold be preserved.
    # [<tt>:jlog_rotate_service_uri</tt>] <tt>"druby://localhost:<em>appropriate_port_number</em>"</tt>.
    #                                     journal log rotation remote service should be enabled.
    #
    # == simple online-backup
    #
    # online-backup is controlled by <tt>higgs_backup</tt> command that
    # is the front end of Higgs::Utils::BackupManager.
    #
    # simple online-backup is like this...
    #
    #   % higgs_backup -v -f foo -t backup_dir -u druby://localhost:appropriate_port_number
    #   2007-09-23 03:00:08.925 [23706]: **** START BACKUP SCENARIO ****
    #   2007-09-23 03:00:08.936 [23706]: start index backup.
    #   2007-09-23 03:00:09.331 [23706]: completed index backup.
    #   2007-09-23 03:00:09.333 [23706]: start data backup.
    #   2007-09-23 03:09:16.663 [23706]: completed data backup.
    #   2007-09-23 03:09:16.692 [23706]: start journal log rotation.
    #   2007-09-23 03:09:17.153 [23706]: completed journal log rotation.
    #   2007-09-23 03:09:17.154 [23706]: start journal logs backup.
    #   2007-09-23 03:09:17.205 [23706]: completed journal logs backup.
    #   2007-09-23 03:09:17.206 [23706]: start backup storage recovery.
    #   2007-09-23 03:09:17.798 [23706]: completed backup storage recovery.
    #   2007-09-23 03:09:17.799 [23706]: start backup storage verify.
    #   2007-09-23 03:25:44.122 [23706]: completed backup storage verify.
    #   2007-09-23 03:25:44.140 [23706]: start journal logs clean.
    #   2007-09-23 03:25:44.541 [23706]: completed journal logs clean.
    #   2007-09-23 03:25:44.542 [23706]: **** COMPLETED BACKUP SCENARIO ****
    #
    # online-backup scenario includes these processes.
    #
    # 1. index backup. see Higgs::Utils::BackupManager#backup_index.
    # 2. data backup. see Higgs::Utils::BackupManager#backup_data.
    # 3. journal log rotation. see Higgs::Utils::BackupManager#rotate_jlog.
    # 4. journal logs backup. see Higgs::Utils::BackupManager#backup_jlog.
    # 5. backup storage recovery. see Higgs::Utils::BackupManager#recover.
    # 6. backup storage verify. see Higgs::Utils::BackupManager#verify.
    # 7. journal logs clean. see Higgs::Utils::BackupManager#clean_jlog.
    #
    class BackupManager
      # for ident(1)
      CVS_ID = '$Id$'

      def initialize(options={})
        @from = options[:from]
        to_dir = options[:to_dir]
        to_name = options[:to_name] || (@from && File.basename(@from))
        @to = File.join(to_dir, to_name) if (to_dir && to_name)
        @jlog_rotate_service_uri = options[:jlog_rotate_service_uri]
        @verbose = options[:verbose] || 0
        @out = options[:out] || STDOUT
      end

      def log(msg)
        t = Time.now
        timestamp = t.strftime('%Y-%m-%d %H:%M:%S')
        milli_sec = format('%03d', t.to_f * 1000 % 1000)
        "#{timestamp}.#{milli_sec} [#{$$}]: #{msg}\n"
      end
      private :log

      def connect_service
        unless (@jlog_rotate_service_uri) then
          raise 'required jlog_rotate_service_uri'
        end
        @out << log("connect to jlog_rotate_service: #{@jlog_rotate_service_uri}") if (@verbose >= 2)
        @jlog_rotate_service = DRbObject.new_with_uri(@jlog_rotate_service_uri)
      end
      private :connect_service

      def backup_index
        @out << log('start index backup.') if (@verbose >= 1)
        unless (@from) then
          raise 'required from_storage'
        end
        unless (@to) then
          raise 'required to_storage'
        end
        connect_service
        @out << log("save to #{@to}.idx") if (@verbose >= 2)
        @jlog_rotate_service.call(File.expand_path(@to) + '.idx')
        @out << log('completed index backup.') if (@verbose >= 1)
        nil
      end

      def backup_data
        @out << log('start data backup.') if (@verbose >= 1)
        unless (@from) then
          raise 'required from_storage'
        end
        unless (@to) then
          raise 'required to_storage'
        end
        FileUtils.cp("#{@from}.tar", "#{@to}.tar", :preserve => true, :verbose => @verbose >= 2)
        @out << log('completed data backup.') if (@verbose >= 1)
        nil
      end

      def rotate_jlog
        @out << log('start journal log rotation.') if (@verbose >= 1)
        connect_service
        @jlog_rotate_service.call(true)
        @out << log('completed journal log rotation.') if (@verbose >= 1)
        nil
      end

      def backup_jlog
        @out << log('start journal logs backup.') if (@verbose >= 1)
        unless (@from) then
          raise 'required from_storage'
        end
        unless (@to) then
          raise 'required to_storage'
        end
        for path in Storage.rotate_entries(@from + '.jlog')
          path =~ /\.jlog\.\d+$/ or raise "mismatch jlog name: #{path}"
          ext = $&
          FileUtils.cp(path, "#{@to}#{ext}", :preserve => true, :verbose => @verbose >= 2)
        end
        @out << log('completed journal logs backup.') if (@verbose >= 1)
        nil
      end

      def recover
        @out << log('start backup storage recovery.') if (@verbose >= 1)
        unless (@to) then
          raise 'required to_storage'
        end
        Storage.recover(@to, @out, @verbose - 1)
        @out << log('completed backup storage recovery.') if (@verbose >= 1)
        nil
      end

      def verify
        @out << log('start backup storage verify.') if (@verbose >= 1)
        unless (@to) then
          raise 'required to_storage'
        end
        st = Storage.new(@to, :read_only => true)
        begin
          st.verify(@out, @verbose - 1)
        ensure
          st.shutdown
        end
        @out << log('completed backup storage verify.') if (@verbose >= 1)
        nil
      end

      def clean_jlog
        @out << log('start journal logs clean.') if (@verbose >= 1)

        unless (@from) then
          raise 'required from_storage'
        end
        unless (@to) then
          raise 'required to_storage'
        end

        for to_jlog in Storage.rotate_entries("#{@to}.jlog")
          to_jlog =~ /\.jlog\.\d+$/ or raise "mismatch jlog name: #{to_jlog}"
          ext = $&
          from_jlog = @from + ext
          if (File.exist? from_jlog) then
            FileUtils.rm(from_jlog, :verbose => @verbose >= 2)
          end
        end

        for to_jlog in Storage.rotate_entries("#{@to}.jlog")
          FileUtils.rm(to_jlog, :verbose => @verbose >= 2)
        end

        @out << log('completed journal logs clean.') if (@verbose >= 1)
        nil
      end

      # run online backup scenario
      def online_backup
        @out << log('**** START BACKUP SCENARIO ****') if (@verbose >= 1)
        backup_index
        backup_data
        rotate_jlog
        backup_jlog
        recover
        verify
        clean_jlog
        @out << log('**** COMPLETED BACKUP SCENARIO ****') if (@verbose >= 1)
        nil
      end
    end
  end
end

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
