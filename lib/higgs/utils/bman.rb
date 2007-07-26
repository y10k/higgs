# backup manager

require 'drb'
require 'fileutils'
require 'higgs/storage'

module Higgs
  module Utils
    # backup manager
    class BackupManager
      # for ident(1)
      CVS_ID = '$Id$'

      def initialize(options={})
        @from = options[:from]
        to_dir = options[:to_dir]
        to_name = options[:to_name] || File.basename(@from)
        @to = File.join(to_dir, to_name)
        @jlog_rotate_service_uri = options[:jlog_rotate_service_uri]
        @verbose = options[:verbose] || 0
        @out = options[:out] || STDOUT
      end

      def log(msg)
        t = Time.now
        "#{t.strftime('%Y-%m-%d %H:%M:%S')}.#{format('%03d', t.to_f * 1000 % 1000)} [#{$$}]: #{msg}\n"
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
        @jlog_rotate_service.call(@to + '.idx')
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
        Storage.recover(@to)
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
