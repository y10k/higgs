# $Id$

require 'digest/sha2'
require 'higgs/cache'
require 'higgs/exceptions'
require 'higgs/tar'
require 'higgs/thread'
require 'yaml'

module Higgs
  class Storage
    # for ident(1)
    CVS_ID = '$Id$'

    include Cache
    include Exceptions

    class Error < HiggsError
    end

    class BrokenError < Error
    end

    class NotWritableError < Error
    end

    class ShutdownException < Exceptions::ShutdownException
    end

    class DebugRollbackException < HiggsException
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
      include Cache

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

        if (options.include? :properties_cache) then
          @properties_cache = options[:properties_cache]
        else
          # require 'higgs/cache'
          @properties_cache = LRUCache.new
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

      @shutdown = false
      @sd_r_lock, @sd_w_lock = Thread::ReadWriteLock.new.to_a

      init_options(options)
      @shared_properties_cache = SharedWorkCache.new(@properties_cache) {|key|
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

    def self.transaction_guard(name)
      class_eval(<<-EOF, "transaction_guard(#{name}) => #{__FILE__}", __LINE__ + 1)
        alias unguarded_#{name} #{name}
        private :unguarded_#{name}

        def #{name}(*args, &block)
	  @sd_r_lock.synchronize{
	    if (@shutdown) then
	      raise ShutdownException, 'storage shutdown'
	    end
	    unguarded_#{name}(*args, &block)
	  }
	end
      EOF
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
      properties = unguarded_fetch_properties(key) or raise BrokenError, "failed to read properties: #{key}"
      content_hash = Digest::SHA512.hexdigest(value)
      if (content_hash != properties['hash']) then
        raise BrokenError, "mismatch content hash at #{key}: expected<#{properties['hash']}> but was <#{content_hash}>"
      end
      value
    end
    transaction_guard :fetch

    def internal_fetch_properties(key)
      properties_yml = read_record_body('p:' + key) or return
      YAML.load(properties_yml)
    end
    private :internal_fetch_properties

    def fetch_properties(key)
      unless (key.kind_of? String) then
        raise TypeError, "can't convert #{key.class} to String"
      end
      @shared_properties_cache[key]
    end
    transaction_guard :fetch_properties

    def key?(key)
      unless (key.kind_of? String) then
        raise TypeError, "can't convert #{key.class} to String"
      end
      @idx_db.key? 'd:' + key
    end
    transaction_guard :key?

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
    transaction_guard :each_key

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
            if (properties = unguarded_fetch_properties(key)) then
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
            @shared_properties_cache.delete(key)
            commit_log['p:' + key] = @w_tar.pos
            @w_tar.add(key + '.properties', properties.to_yaml, :mtime => commit_time)
          when :delete
            @shared_properties_cache.delete(key)
            if (@idx_db.key? 'd:' + key) then
              commit_log['d:' + key] = :delete
            end
            if (@idx_db.key? 'p:' + key) then
              commit_log['p:' + key] = :delete
            end
          when :update_properties
            if (properties = new_properties[key]) then
              # nothing to do.
            elsif (properties = unguarded_fetch_properties(key)) then
              # nothing to do.
            else
              # KeyError : ruby 1.9 feature
              key_error = (defined? KeyError) ? KeyError : IndexError
              raise key_error, "not exist properties at key: #{key}"
            end
            @shared_properties_cache.delete(key)
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
    transaction_guard :write_and_commit

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

    def _block_alive?(head, pos)
      key = 'd:' + head[:name]
      if (read_index(key) == pos) then
	return key
      end

      if (head[:name] =~ /\.properties$/) then
	key = 'p:' + $`
	if (read_index(key) == pos) then
	  return key
	end
      end

      nil
    end
    private :_block_alive?

    def reorganize
      if (@read_only) then
        raise NotWritableError, 'failed to reorganize read only storage'
      end

      @r_tar_pool.transaction{|r_tar|
        curr_pos = 0
        r_tar.seek(0)
        while (head = r_tar.read_header(true))
          if (block_alive? head, curr_pos) then
	    p [ :debug, :reorganize, :skip_alive, :curr_pos, curr_pos ] if $DEBUG
            curr_pos = r_tar.pos
          else
	    p [ :debug, :reorganize, :shift, :curr_pos, curr_pos ] if $DEBUG
            if (next_pos = reorganize_shift(r_tar, curr_pos)) then
              r_tar.seek(next_pos)
              curr_pos = next_pos
            else
	      p [ :debug, :reorganize, :eoa, :curr_pos, curr_pos ] if $DEBUG
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
    transaction_guard :reorganize

    FETCH_DATA_KEY = proc{|head|
      'd:' + head[:name]
    }

    FETCH_PROPERTIES_KEY = proc{|head|
      'p:' + head[:name].sub(/\.properties$/, '')
    }

    SHIFT_BUNDLE_SIZE = 1024 * 64

    class Bundle
      include Enumerable

      def initialize
	@attrs_alist = []
	@size = 0
	@buffer = ''
      end

      attr_reader :size

      def add(key, size, offset)
	blocked_size = Tar::Block::BLKSIZ             # head
	blocked_size += Tar::Block.blocked_size(size) # body
	@attrs_alist << [ key, blocked_size, offset ]
	@size += blocked_size
	self
      end

      def each
	for key, blocked_size, offset in @attrs_alist
	  yield(key, blocked_size)
	end
	self
      end

      def read(r_tar)
	r_io = r_tar.to_io
	for key, blocked_size, offset in @attrs_alist
	  r_io.seek(offset)
	  @buffer << r_io.read(blocked_size)
	end
	self
      end

      def to_s
	@buffer
      end
    end

    def reorganize_shift(r_tar, offset)
      p [ :debug, :reorganize_shift, :offset, offset ] if $DEBUG

      shift_bundle = Bundle.new
      curr_pos = offset
      r_tar.seek(offset)
      r_tar.each(true) do |head|
        if (key = _block_alive?(head, curr_pos)) then
	  shift_bundle.add(key, head[:size], curr_pos)
	  if (shift_bundle.size > SHIFT_BUNDLE_SIZE) then
	    break
	  end
        end
        curr_pos = r_tar.pos
      end
      if (shift_bundle.size == 0) then
        return
      end
      p [ :debug, :reorganize_shift, :shift_bundle, shift_bundle ] if $DEBUG
      shift_bundle.read(r_tar)

      gap_bundle = Bundle.new
      move_tail_bundle = Bundle.new
      curr_pos = offset
      r_tar.seek(offset)
      r_tar.each(true) do |head|
	if (key = _block_alive?(head, curr_pos)) then
	  gap_bundle.add(key, head[:size], curr_pos)
	  move_tail_bundle.add(key, head[:size], curr_pos)
	else
	  gap_bundle.add(:no_key, head[:size], curr_pos)
	end
	if (gap_bundle.size >= shift_bundle.size) then
	  break
	end
	curr_pos = r_tar.pos
      end

      p [ :debug, :reorganize_shift, :gap_bundle, gap_bundle ] if $DEBUG
      p [ :debug, :reorganize_shift, :move_tail_bundle, move_tail_bundle ] if $DEBUG

      if (move_tail_bundle.size > 0) then
	move_tail_bundle.read(r_tar)
	eoa = @idx_db['EOA'].to_i
	w_io = @w_tar.to_io
	w_io.seek(eoa)
	w_io.write(move_tail_bundle.to_s)
	@w_tar.write_EOA
	@io_sync.call(@w_tar)
	@idx_db['EOA'] = (eoa + move_tail_bundle.size).to_s
	@idx_db.sync
	curr_pos = eoa
	for key, blocked_size in move_tail_bundle
	  p [ :debug, :reorganize_shift, :move_tail, key, :pos, curr_pos, :size, blocked_size ] if $DEBUG
	  @idx_db[key] = curr_pos.to_s
	  curr_pos += blocked_size
	end
	@idx_db.sync
      end

      if (gap_bundle.size >= shift_bundle.size) then
	eoa = @idx_db['EOA'].to_i
	w_io = @w_tar.to_io
	w_io.seek(offset)
	w_io.write(shift_bundle.to_s)
	@io_sync.call(@w_tar)
	curr_pos = offset
	for key, blocked_size in shift_bundle
	  p [ :debug, :reorganize_shift, :shift, key, :pos, curr_pos, :size, blocked_size ] if $DEBUG
	  @idx_db[key] = curr_pos.to_s
	  curr_pos += blocked_size
	end
	@idx_db.sync

	if (gap_bundle.size > shift_bundle.size) then
	  @w_tar.seek(offset + shift_bundle.size)
	  gap_size = gap_bundle.size - shift_bundle.size
	  gap_size -= Tar::Block::BLKSIZ # header size
	  if (gap_size < 0) then
	    raise "Bug: mismatch negative gap size: #{gap_size}"
	  end
	  p [ :debug, :reorganize_shift, :gap, :pos, @w_tar.pos, :size_no_head, gap_size ] if $DEBUG
	  @w_tar.write_header(:name => '.gap', :size => gap_size)
	  @io_sync.call(@w_tar)
	end
      else
	# retry
	return offset
      end

      offset + shift_bundle.size
    end
    private :reorganize_shift

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
    transaction_guard :dump

    def verify_each_key
      yield('.higgs')
      unguarded_each_key do |key|
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
        unguarded_fetch(key) or raise BrokenError, "not found: #{key}"

        if (@idx_db['d:' + key].to_i >= eoa) then
          raise BrokenError, "too large data index: #{key}"
        end
        if (@idx_db['p:' + key].to_i >= eoa) then
          raise BrokenError, "too large properties index: #{key}"
        end
      end
    end
    transaction_guard :verify

    def shutdown
      @sd_w_lock.synchronize{
	if (@shutdown) then
	  raise ShutdownException, 'storage shutdown'
	end
	@shutdown = true

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
      }
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
	(@write_map[key] != :delete) &&
	((@local_cache.key? key) && ! @local_cache[key].nil? || (@storage.key? key))
      end

      def each_key
	@local_cache.each_key do |key|
	  lock(key)
	  if (@write_map[key] != :delete && ! @local_cache[key].nil?) then
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
	@write_map.map{|key, ope|
	  case (ope)
	  when :delete
	    [ key, ope ]
	  else
	    [ key, ope, @local_cache[key] ]
	  end
	}
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
