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

    class NotWritableError < Error
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

    class DebugRollbackLogDeletedException < DebugRollbackException
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

        if (options.include? :cache_type) then
          @cache_type = options[:cache_type]
        else
          require 'higgs/cache'
          @cache_type = Cache::SharedWorkCache
        end

	if (options.include? :fsync) then
	  @fsync = options[:fsync]
	else
	  @fsync = true
	end
      end
      private :init_options

      attr_reader :read_only
      attr_reader :number_of_read_io
      attr_reader :fsync
    end
    include InitOptions

    def initialize(name, options={})
      @name = name
      @tar_name = "#{@name}.tar"
      @idx_name = "#{@name}.idx"

      init_options(options)
      @properties_read_cache = @cache_type.new{|key|
        internal_fetch_properties(key)
      }

      @idx_db_opened = false
      @w_tar_opened = false
      @io_sync = (@fsync) ? proc{|io| io.fsync } : proc{|io| io.flush }
      begin
        if (init_io) then
          build_storage_at_first_time
        else
          rollback
        end
      rescue
        shutdown
        raise
      end
    end

    def init_io
      if (@read_only) then
        @idx_db = @dbm_read_open.call(@idx_name)
        @idx_db_opened = true
      else
        @idx_db = @dbm_write_open.call(@idx_name)
        @idx_db_opened = true
        begin
          w_io = File.open(@tar_name, File::WRONLY | File::CREAT | File::EXCL, 0660)
          first_time = true
        rescue Errno::EEXIST
          w_io = File.open(@tar_name, File::WRONLY, 0660)
          first_time = false
        end
        @w_tar = Tar::ArchiveWriter.new(w_io)
        @w_tar_opened = true
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
      @r_tar_pool.transaction{|r_tar|
        r_tar.seek(pos)
        r_tar.fetch or raise BrokenError, "failed to read record: #{key}" 
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
      properties = fetch_properties(key) or raise BrokenError, "failed to read properties: #{key}"
      content_hash = Digest::SHA512.hexdigest(value)
      if (content_hash != properties['hash']) then
        raise BrokenError, "mismatch content hash at #{key}: expected<#{properties['hash']}> but was <#{content_hash}>"
      end
      value
    end

    def internal_fetch_properties(key)
      properties_yml = read_record_body('p:' + key) or return
      YAML.load(properties_yml)
    end
    private :internal_fetch_properties

    def fetch_properties(key)
      unless (key.kind_of? String) then
        raise TypeError, "can't convert #{key.class} to String"
      end
      @properties_read_cache[key]
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
      if (@read_only) then
        raise NotWritableError, 'failed to write to read only storage'
      end

      commit_time = Time.now
      committed = false
      commit_log = {}
      rollback_log = {}
      new_properties = {}

      __debug_rollback_before_rollback_log_write__ = false
      __debug_rollback_after_rollback_log_write__ = false
      __debug_rollback_after_commit_log_write__ = false
      __debug_rollback_commit_completed__ = false
      __debug_rollback_log_deleted__ = false

      begin
        eoa = @idx_db['EOA'].to_i
        rollback_log[:EOA] = eoa
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
            @properties_read_cache.delete(key)
            commit_log['p:' + key] = @w_tar.pos
            @w_tar.add(key + '.properties', properties.to_yaml, :mtime => commit_time)
          when :delete
            @properties_read_cache.delete(key)
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
              # KeyError : ruby 1.9 feature
              key_error = (defined? KeyError) ? KeyError : IndexError
              raise key_error, "not exist properties at key: #{key}"
            end
            @properties_read_cache.delete(key)
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
          when :__debug_rollback_log_deleted__
            __debug_rollback_log_deleted__ = true
          else
            raise ArgumentError, "unknown operation: #{ope}"
          end
        end

        eoa = @w_tar.pos
        @w_tar.write_EOA
        @io_sync.call(@w_tar)

        if (__debug_rollback_before_rollback_log_write__) then
          raise DebugRollbackBeforeRollbackLogWriteException, 'debug'
        end

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

        if (__debug_rollback_commit_completed__) then
          raise DebugRollbackCommitCompletedException, 'debug'
        end

        @idx_db.delete('rollback')
        @idx_db.sync

        if (__debug_rollback_log_deleted__) then
          raise DebugRollbackLogDeletedException, 'debug'
        end

        committed = true
      ensure
        rollback unless committed
      end
      nil
    end

    def rollback
      if (@read_only) then
        if (@idx_db.key? 'rollback') then
          raise NotWritableError, 'failed to rollback to read only storage'
        end
      end

      if (rollback_dump = @idx_db['rollback']) then
        rollback_log = Marshal.load(rollback_dump)
        rollback_eoa = rollback_log.delete(:EOA) or raise BrokenError, 'invalid rollback log'
        eoa = @idx_db['EOA'].to_i

        if (eoa == rollback_eoa) then
          rollback_log.each_pair do |key, pos|
            case (pos)
            when :new
              @idx_db.delete(key)
            else
              if (pos >= eoa) then
                raise BrokenError, 'invalid rollback log'
              end
              roll_forward_pos = read_index(key)
              @idx_db[key] = pos.to_s
            end
          end
          @idx_db.sync

          @w_tar.seek(eoa)
          @w_tar.write_EOA
          @io_sync.call(@w_tar)
        elsif (eoa > rollback_eoa) then
          # roll forward
        else
          # if (eoa < rollback_eoa) then
          raise BrokenError, 'shrinked storage'
        end

        @idx_db.delete('rollback')
        @idx_db.sync
      end

      nil
    end
    private :rollback

    def block_alive?(head, pos)
      if (read_index('d:' + head[:name]) == pos) then
        return :data
      end
      if (head[:name] =~ /\.properties$/) then
        if (read_index('p:' + head[:name].sub(/\.properties$/, '')) == pos) then
          return :properties
        end
      end
      nil
    end
    private :block_alive?

    def reorganize
      if (@read_only) then
        raise NotWritableError, 'failed to reorganize read only storage'
      end

      @r_tar_pool.transaction{|r_tar|
        curr_pos = 0
        r_tar.seek(0)
        while (head = r_tar.read_header(true))
          p [ :debug, :reorganize, :curr_pos, curr_pos ] if $DEBUG
          if (block_alive? head, curr_pos) then
            curr_pos = r_tar.pos
          else
            if (next_pos = reorganize_shift(r_tar, curr_pos)) then
              r_tar.seek(next_pos)
              curr_pos = next_pos
            else
              # end of reorganize
              @idx_db['EOA'] = curr_pos.to_s
              @idx_db.sync
              @w_tar.seek(curr_pos)
              @w_tar.write_EOA
              @io_sync.call(@w_tar)
              @w_tar.truncate(curr_pos + Tar::Block::BLKSIZ * 2)
              break
            end
          end
        end
      }
      nil
    end

    FETCH_DATA_KEY = proc{|head|
      'd:' + head[:name]
    }

    FETCH_PROPERTIES_KEY = proc{|head|
      'p:' + head[:name].sub(/\.properties$/, '')
    }

    def reorganize_shift(r_tar, offset)
      p [ :debug, :reorganize_shift, :offset, offset ] if $DEBUG

      alive_head = nil
      alive_pos = nil
      fetch_key = nil
      curr_pos = r_tar.pos
      r_tar.each(true) do |head|
        if (type = block_alive?(head, curr_pos)) then
          alive_head = head
          alive_pos = curr_pos
          case (type)
          when :data
            fetch_key = FETCH_DATA_KEY
          when :properties
            fetch_key = FETCH_PROPERTIES_KEY
          else
            raise "unknown type: #{type}"
          end
          break
        end
        curr_pos = r_tar.pos
      end

      unless (alive_head) then
        return
      end

      p [ :debug, :reorganize_shift, :gap, offset, alive_pos ] if $DEBUG

      gap_size = alive_pos - offset
      alive_size = Tar::Block::BLKSIZ + Tar::Block.blocked_size(alive_head[:size])

      # ^ <-- offset
      # |
      # | gap_size
      # |
      # V
      # ^ <-- alive_pos
      # |
      # | Tar::Block::BLKSIZ (header)
      # |
      # V
      # ^
      # |
      # | Tar::Block.blocked_size(alive_head[:size])
      # |
      # V

      if (gap_size >= alive_size + Tar::Block::BLKSIZ) then
        puts "debug: reorganize_shift: gap_size(#{gap_size}) >= alive_size(#{alive_size}) + Tar::Block::BLKSIZ" if $DEBUG
        @w_tar.seek(offset + alive_size)
        @w_tar.write_header(:name => '.gap', :size => gap_size - alive_size - Tar::Block::BLKSIZ)
        @io_sync.call(@w_tar)
        copy(r_tar, alive_size, alive_pos, offset)
        @idx_db[fetch_key.call(alive_head)] = offset.to_s
        @idx_db.sync
        return offset + alive_size
      elsif (gap_size == alive_size) then
        puts "debug: reorganize_shift: gap_size(#{gap_size}) == alive_size(#{alive_size})" if $DEBUG
        copy(r_tar, alive_size, alive_pos, offset)
        @idx_db[fetch_key.call(alive_head)] = offset.to_s
        @idx_db.sync
        return offset + alive_size
      else
        puts 'debug: reorganize_shift: EOA' if $DEBUG
        eoa = @idx_db['EOA'].to_i
        copy(r_tar, alive_size, alive_pos, eoa)
        next_eoa = eoa + alive_size
        @w_tar.seek(next_eoa)
        @w_tar.write_EOA
        @io_sync.call(@w_tar)
        @idx_db['EOA'] = next_eoa.to_s
        @idx_db.sync
        @idx_db[fetch_key.call(alive_head)] = eoa.to_s
        @idx_db.sync
        return offset
      end

      raise 'Bug: not to reach'
    end
    private :reorganize_shift

    def copy(r_tar, size, from_pos, to_pos)
      r_io = r_tar.to_io
      r_io.seek(from_pos)
      data = r_io.read(size)
      w_io = @w_tar.to_io
      w_io.seek(to_pos)
      w_io.write(data)
      @io_sync.call(w_io)
      w_io.fsync
      nil
    end
    private :copy

    def dump(out=STDOUT)
      @r_tar_pool.transaction{|r_tar|
        curr_pos = 0
        r_tar.seek(0)
        r_tar.each(true) do |head|
          if (type = block_alive?(head, curr_pos)) then
            case (type)
            when :data
              out << [ :data, curr_pos, head ].inspect << "\n"
            when :properties
              out << [ :properties, curr_pos, head ].inspect << "\n"
            else
              raise "unknown type: #{type}"
            end
          else
            out << [ :gap, curr_pos, head ].inspect << "\n"
          end
          curr_pos = r_tar.pos
        end
      }
      nil
    end

    def verify_each_key
      yield('.higgs')
      each_key do |key|
        yield(key)
      end
      self
    end
    private :verify_each_key

    def verify
      if (@idx_db.key? 'rollback') then
        raise NotWritableError, 'need to rollback'
      end

      unless (@idx_db.key? 'EOA') then
        raise BrokenError, 'not found a EOA index'
      end
      eoa = @idx_db['EOA'].to_i

      verify_each_key do |key|
        # hash check
        fetch(key) or raise BrokenError, "not found: #{key}"

        if (@idx_db['d:' + key].to_i >= eoa) then
          raise BrokenError, "too large data index: #{key}"
        end
        if (@idx_db['p:' + key].to_i >= eoa) then
          raise BrokenError, "too large properties index: #{key}"
        end
      end
    end

    def shutdown
      if (@w_tar_opened) then
        @io_sync.call(@w_tar)
        @w_tar.close(true)
      end

      if (@r_tar_pool) then
        @r_tar_pool.shutdown{|r_tar|
          r_tar.close
        }
      end

      if (@idx_db_opened) then
        @idx_db.sync unless @read_only
        @idx_db.close
      end

      nil
    end

    class TransactionContext
      def initialize(storage, read_cache, lock_handler)
	@storage = storage
	@read_cache = read_cache
	@local_cache = Hash.new{|hash, key| hash[key] = @read_cache[key] }
	@lock_handler = lock_handler
	@locked_map = {}
	@locked_map.default = false
	@write_map = {}
      end

      def locked?(key)
	@locked_map[key]
      end

      def lock(key)
	unless (@locked_map[key]) then
	  @lock_handler.lock(key)
	  @locked_map[key] = true
	end
	nil
      end

      def unlock(key)
	if (@locked_map[key]) then
	  @lock_handler.unlock(key)
	  @locked_map[key] = false
	end
	nil
      end

      def [](key)
	lock(key)
	(@write_map[key] != :delete) ? @local_cache[key] : nil
      end

      def []=(key, value)
	lock(key)
	@write_map[key] = :write
	@read_cache.delete(key)
	@local_cache[key] = value
      end

      def delete(key)
	lock(key)
	if (@write_map[key] != :delete) then
	  @write_map[key] = :delete
	  @local_cache[key]	# load from storage
	  @read_cache.delete(key)
	  @local_cache.delete(key)
	end
      end

      def key?(key)
	lock(key)
	(@write_map[key] != :delete) && ((@local_cache.key? key) || (@storage.key? key))
      end

      def each_key
	@local_cache.each_key do |key|
	  lock(key)
	  if (@write_map[key] != :delete)then
	    yield(key)
	  end
	end
	@storage.each_key do |key|
	  lock(key)
	  if ((@write_map[key] != :delete) && ! (@local_cache.key? key)) then
	    yield(key)
	  end
	end
	self
      end

      def write_list
	@write_map.map{|key, ope| [ key, ope, @local_cache[key] ] }
      end

      def write_clear
	@write_map.clear
	nil
      end

      def rollback
	@local_cache.clear
	@write_map.clear
	nil
      end
    end
  end
end
