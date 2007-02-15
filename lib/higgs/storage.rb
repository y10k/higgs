# $Id$

require 'digest/sha2'
require 'higgs/tar'
require 'higgs/thread'
require 'yaml'

module Higgs
  class Storage
    # for ident(1)
    CVS_ID = '$Id$'

    class Error < StandardError
    end

    class BrokenError < Error
    end

    class DebugRollbackException < Exception
    end

    class DebugRollbackBeforeRollbackLogWriteException < DebugRollbackException
    end

    class DebugRollbackAfterRollbackLogWriteException < DebugRollbackException
    end

    class DebugRollbackAfterCommitLogWriteException < DebugRollbackException
    end

    class DebugRollbackCommitCompletedException < DebugRollbackException
    end

    module InitOptions
      def init_options(options)
        @number_of_read_io = options[:number_of_read_io] || 2

        if (options.include? :read_only) then
          @read_only = options[:read_only]
        else
          @read_only = false
        end

        if (options.include? :dbm_open) then
          @dbm_read_open = options[:dbm_open][:read]
          @dbm_write_open = options[:dbm_open][:write]
        else
          require 'higgs/index/gdbm'
          @dbm_read_open = Index::GDBM_OPEN[:read]
          @dbm_write_open = Index::GDBM_OPEN[:write]
        end
      end
      private :init_options
    end
    include InitOptions

    def initialize(name, options={})
      @name = name
      @tar_name = "#{@name}.tar"
      @idx_name = "#{@name}.idx"
      init_options(options)
      if (init_io) then
        build_storage_at_first_time
      else
        rollback
      end
    end

    def init_io
      if (@read_only) then
        @idx_db = @dbm_read_open.call(@idx_name)
      else
        @idx_db = @dbm_write_open.call(@idx_name)
        begin
          w_io = File.open(@tar_name, File::WRONLY | File::CREAT | File::EXCL, 0660)
          first_time = true
        rescue Errno::EEXIST
          w_io = File.open(@tar_name, File::WRONLY, 0660)
          first_time = false
        end
        @w_tar = Tar::ArchiveWriter.new(w_io)
      end
      @r_tar_pool = Thread::Pool.new(@number_of_read_io) {
        Tar::ArchiveReader.new(Tar::RawIO.new(File.open(@tar_name, File::RDONLY)))
      }
      first_time
    end
    private :init_io

    def build_storage_at_first_time
      @idx_db['EOA'] = '0'
      write_list = []
      storage_info = {
        'version' => {
          'major' => 0,
          'minor' => 0
        },
        'cvs_id' => '$Id$',
        'build_time' => Time.now,
        'hash_type' => 'SHA512'
      }
      write_list << [ '.higgs', :write, storage_info.to_yaml ]
      write_and_commit(write_list)
    end
    private :build_storage_at_first_time

    def read_index(key)
      pos = @idx_db[key] or return
      pos.to_i
    end
    private :read_index

    def read_record(key)
      pos = read_index(key) or return
      head_and_body = nil
      @r_tar_pool.transaction{|r_tar|
        r_tar.seek(pos)
        r_tar.fetch or raise BrokenError, "broken storage and failed to read record: #{key}" 
      }
    end
    private :read_record

    def read_record_body(key)
      head_and_body = read_record(key) or return
      head_and_body[:body]
    end
    private :read_record_body

    def fetch(key)
      unless (key.kind_of? String) then
        raise TypeError, "can't convert #{key.class} to String"
      end
      value = read_record_body('d:' + key) or return
      properties = fetch_properties(key) or raise BrokenError, "broken storage and failed to read properties: #{key}"
      content_hash = Digest::SHA512.hexdigest(value)
      if (content_hash != properties['hash']) then
        raise BrokenError, "broken storage and mismatch content hash at #{key}: expected<#{content_hash}> but was <#{properties['hash']}>"
      end
      value
    end

    def fetch_properties(key)
      unless (key.kind_of? String) then
        raise TypeError, "can't convert #{key.class} to String"
      end
      properties_yml = read_record_body('p:' + key) or return
      YAML.load(properties_yml)
    end

    def key?(key)
      unless (key.kind_of? String) then
        raise TypeError, "can't convert #{key.class} to String"
      end
      @idx_db.key? 'd:' + key
    end

    def each_key
      @idx_db.each_key do |key|
        key = key.dup
        if (key.sub!(/^d:/, '')) then
          if (key != '.higgs') then
            yield(key)
          end
        end
      end
      self
    end

    def write_and_commit(write_list)
      commit_time = Time.now
      commit_log = {}
      committed = false
      new_properties = {}

      __debug_rollback_before_rollback_log_write__ = false
      __debug_rollback_after_rollback_log_write__ = false
      __debug_rollback_after_commit_log_write__ = false
      __debug_rollback_commit_completed__ = false

      begin
        eoa = @idx_db['EOA'].to_i
        @w_tar.seek(eoa)

        for key, ope, value in write_list
          unless (key.kind_of? String) then
            raise TypeError, "can't convert #{key.class} (key) to String"
          end
          case (ope)
          when :write
            unless (value.kind_of? String) then
              raise TypeError, "can't convert #{value.class} (value) to String"
            end
            commit_log['d:' + key] = @w_tar.pos
            @w_tar.add(key, value, :mtime => commit_time)
            if (properties = fetch_properties(key)) then
              properties['hash'] = Digest::SHA512.hexdigest(value)
              new_properties[key] = properties
            else
              properties = {
                'hash' => Digest::SHA512.hexdigest(value),
                'created_time' => commit_time,
                'custom_properties' => {}
              }
              new_properties[key] = properties
            end
            commit_log['p:' + key] = @w_tar.pos
            @w_tar.add(key + '.properties', properties.to_yaml, :mtime => commit_time)
          when :delete
            if (@idx_db.key? 'd:' + key) then
              commit_log['d:' + key] = :delete
            end
            if (@idx_db.key? 'p:' + key) then
              commit_log['p:' + key] = :delete
            end
          when :update_properties
            if (properties = new_properties[key]) then
              # nothing to do.
            elsif (properties = fetch_properties(key)) then
              # nothing to do.
            else
              raise BrokenError, "broken storage and not exist properties: #{key}"
            end
            properties['custom_properties'] = value
            new_properties[key] = properties
            commit_log['p:' + key] = @w_tar.pos
            @w_tar.add(key + '.properties', properties.to_yaml, :mtime => commit_time)
          when :__debug_rollback_before_rollback_log_write__
            __debug_rollback_before_rollback_log_write__ = true
          when :__debug_rollback_after_rollback_log_write__
            __debug_rollback_after_rollback_log_write__ = true
          when :__debug_rollback_after_commit_log_write__
            __debug_rollback_after_commit_log_write__ = true
          when :__debug_rollback_commit_completed__
            __debug_rollback_commit_completed__ = true
          else
            raise ArgumentError, "unknown operation: #{ope}"
          end
        end

        eoa = @w_tar.pos
        @w_tar.write_EOA
        @w_tar.fsync

        if (__debug_rollback_before_rollback_log_write__) then
          raise DebugRollbackBeforeRollbackLogWriteException, 'debug'
        end

        rollback_log = {}
        commit_log.each_key do |key|
          if (pos = read_index(key)) then
            rollback_log[key] = pos.to_i
          else
            rollback_log[key] = :new
          end
        end
        @idx_db['rollback'] = Marshal.dump(rollback_log)
        @idx_db.sync

        if (__debug_rollback_after_rollback_log_write__) then
          raise DebugRollbackAfterRollbackLogWriteException, 'debug'
        end

        commit_log.each_pair do |key, pos|
          case (pos)
          when :delete
            @idx_db.delete(key)
          else
            @idx_db[key] = pos.to_s
          end
        end
        @idx_db.sync

        if (__debug_rollback_after_commit_log_write__) then
          raise DebugRollbackAfterCommitLogWriteException, 'debug'
        end

        @idx_db['EOA'] = eoa.to_s
        @idx_db.sync
        @idx_db.delete('rollback')
        @idx_db.sync

        if (__debug_rollback_commit_completed__) then
          raise DebugRollbackCommitCompletedException, 'debug'
        end

        committed = true
      ensure
        rollback unless committed
      end
      nil
    end

    def rollback
      if (rollback_dump = @idx_db['rollback']) then
        rollback_log = Marshal.dump(rollback_dump)
        eoa = @idx_db['EOA'].to_i

        rollback_log.each_pair do |key, pos|
          case (pos)
          when :new
            @idx_db.delete(key)
          else
            if (pos > eoa) then
              raise BrokenError, 'broken storage and invalid rollback log'
            end
            roll_forward_pos = read_index(key)
            if (roll_forward_pos.nil? || roll_forward_pos > eoa) then
              @idx_db[key] = pos.to_s
            end
          end
        end
        @idx_db.sync

        @idx_db.delete('rollback')
        @idx_db.sync

        @w_tar.seek(eoa)
        @w_tar.write_EOA
        @w_tar.fsync
      end
      nil
    end

    def shutdown
      @idx_db.sync
      @idx_db.close
      @w_tar.fsync
      @w_tar.close(true)
      @r_tar_pool.shutdown{|r_tar| r_tar.close }
      nil
    end
  end
end
