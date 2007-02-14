# $Id$

require 'digest/sha2'
require 'higgs/tar'
require 'higgs/thread'
require 'yaml'

module Higgs
  class Storage
    # for ident(1)
    CVS_ID = '$Id$'

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
      storage_information = {
        'version' => {
          'major' => 0,
          'minor' => 0
        },
        'cvs_id' => '$Id$',
        'build_time' => Time.now,
        'hash_type' => 'SHA512'
      }

      data = storage_information.to_yaml
      properties = {
        'hash' => Digest::SHA512.hexdigest(data),
        'created_time' => Time.now,
        'custom_properties' => {}
      }

      @idx_db['d:.higgs'] = 0.to_s
      @w_tar.add('.higgs', data, :mtime => properties['created_time'])

      @idx_db['p:.higgs'] = @w_tar.pos.to_s
      @w_tar.add('.higgs.properties', properties.to_yaml, :mtime => properties['created_time'])
      @idx_db['EOA'] = @w_tar.pos.to_s

      @w_tar.fsync
      @idx_db.sync
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
        r_tar.fetch or raise "failed to read record: #{key}"
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
      properties = fetch_properties(key) or raise "failed to read properties: #{key}"
      content_hash = Digest::SHA512.hexdigest(value)
      if (content_hash != properties['hash']) then
        raise "mismatch content hash at #{key}: expected<#{content_hash}> but was <#{properties['hash']}>"
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

    def write_and_commit(write_list)
      commit_time = Time.now
      commit_log = {}
      committed = false

      begin
        eoa = @idx_db['EOA'].to_i
        @w_tar.seek(eoa)

        for key, ope, value in write_list
          unless (key.kind_of? String) then
            raise TypeError, "can't convert #{key.class} to String"
          end
          case (ope)
          when :write
            commit_log['d:' + key] = @w_tar.pos
            @w_tar.add(key, value, :mtime => commit_time)
            if (properties = fetch_properties(key)) then
              properties['hash'] = Digest::SHA512.hexdigest(value)
            else
              properties = {
                'hash' => Digest::SHA512.hexdigest(value),
                'created_time' => commit_time,
                'custom_properties' => {}
              }
            end
            commit_log['p:' + key] = @w_tar.pos
            @w_tar.add(key + '.properties', properties.to_yaml, :mtime => commit_time)
          when :delete
            raise NotImplementedError, 'not implemented delete operation'
          when :update_properties
            raise NotImplementedError, 'not implemented update_properties operation'
          else
            raise "unknown operation: #{ope}"
          end
        end

        eoa = @w_tar.pos
        @w_tar.write_EOA
        @w_tar.fsync

        rollback_log = {}
        commit_log.each_key do |key|
          if (pos = @idx_db[key]) then
            rollback_log[key] = pos.to_i
          else
            rollback_log[key] = :new
          end
        end
        @idx_db['rollback'] = Marshal.dump(rollback_log)
        @idx_db.sync

        commit_log.each_pair do |key, pos|
          @idx_db[key] = pos.to_i
        end
        @idx_db.sync

        @idx_db['EOA'] = eoa.to_s
        @idx_db.sync
        @idx_db.delete('rollback')
        @idx_db.sync
        committed = true
      ensure
        rollback unless committed
      end
      nil
    end

    def rollback
      raise NotImplementedError, 'not implemented rollback'
    end

    def shutdown
      @w_tar.seek(@idx_db['EOA'].to_i)
      @w_tar.write_EOA

      @idx_db.sync
      @idx_db.close
      @w_tar.fsync
      @w_tar.close(true)
      @r_tar_pool.shutdown{|r_tar| r_tar.close }
      nil
    end
  end
end
