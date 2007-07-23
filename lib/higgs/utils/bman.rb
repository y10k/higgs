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
        @to_dir = options[:to_dir]
        @to_name = options[:to_name] || File.basename(@from)
        @jlog_rotate_service_uri = options[:jlog_rotate_service_uri]
        @verbose = options[:verbose] || 0
        @out = options[:out] || STDOUT
      end

      def connect_service
        unless (@jlog_rotate_service_uri) then
          raise 'required jlog_rotate_service_uri'
        end
        @jlog_rotate_service = DRbObject.new_with_uri(@jlog_rotate_service_uri)
      end
      private :connect_service

      def backup_index
        connect_service
        @jlog_rotate_service.call(File.join(@to_dir, @to_name) + '.idx')
        nil
      end

      def backup_data
        unless (@from) then
          raise 'required from_storage'
        end
        unless (@to_dir && @to_name) then
          raise 'required to_storage'
        end
        FileUtils.cp(@from + '.tar', File.join(@to_dir, @to_name) + '.tar', :preserve => true)
        nil
      end

      def rotate_jlog
        connect_service
        @jlog_rotate_service.call(true)
        nil
      end

      def backup_jlog
        unless (@from) then
          raise 'required from_storage'
        end
        unless (@to_dir && @to_name) then
          raise 'required to_storage'
        end
        for path in Storage.rotate_entries(@from + '.jlog')
          path =~ /\.jlog\.\d+$/ or raise "mismatch jlog name: #{path}"
          ext = $&
          FileUtils.cp(path, File.join(@to_dir, @to_name) + ext, :preserve => true)
        end
        nil
      end

      def recover
        unless (@to_dir && @to_name) then
          raise 'required to_storage'
        end
        Storage.recover(File.join(@to_dir, @to_name))
        nil
      end

      def verify
        unless (@to_dir && @to_name) then
          raise 'required to_storage'
        end
        st = Storage.new(File.join(@to_dir, @to_name), :read_only => true)
        begin
          st.verify
        ensure
          st.shutdown
        end
        nil
      end

      def clean_jlog
        unless (@from) then
          raise 'required from_storage'
        end
        unless (@to_dir && @to_name) then
          raise 'required to_storage'
        end

        for to_jlog in Storage.rotate_entries(File.join(@to_dir, @to_name + '.jlog'))
          to_jlog =~ /\.jlog\.\d+$/ or raise "mismatch jlog name: #{to_jlog}"
          ext = $&
          from_jlog = @from + ext
          if (File.exist? from_jlog) then
            FileUtils.rm(from_jlog)
          end
        end

        for to_jlog in Storage.rotate_entries(File.join(@to_dir, @to_name + '.jlog'))
          FileUtils.rm(to_jlog)
        end

        nil
      end

      # run online backup scenario
      def online_backup
        backup_index
        backup_data
        rotate_jlog
        backup_jlog
        recover
        verify
        clean_jlog
        nil
      end
    end
  end
end

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
