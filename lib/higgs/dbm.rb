# $Id$

require 'forwardable'
require 'higgs/cache'
require 'thread'

module Higgs
  class DBM
    # for ident(1)
    CVS_ID = '$Id$'

    extend Forwardable
    include Cache

    module InitOptions
      include Cache

      def init_options(options)
        if (options.include? :read_only) then
          @read_only = options[:read_only]
        else
          @read_only = false
        end

        if (options.include? :storage_type) then
          @storage_type = options[:storage_type]
        else
          require 'higgs/storage'
          @storage_type = Storage
        end

        if (options.include? :read_cache) then
          @read_cache = options[:read_cache]
        else
          # require 'higgs/cache'
          @read_cache = LRUCache.new
        end

        if (options.include? :lock_manager) then
          @lock_manager = options[:lock_manager]
        else
          require 'higgs/lock'
          @lock_manager = Lock::FineGrainLockManager.new
        end
      end
      private :init_options

      attr_reader :read_only
    end
    include InitOptions

    def initialize(name, options={})
      @name = name
      init_options(options)
      @storage = @storage_type.new(name, options)
      @shared_read_cache = SharedWorkCache.new(@read_cache) {|key|
        @storage.fetch(key)
      }
      @commit_lock = Mutex.new
    end

    def_delegator :@storage, :shutdown

    def transaction(read_only=@read_only)
      r = nil
      if (read_only) then
	@lock_manager.transaction(true) {|lock_handler|
	  tx = ReadTransactionContext.new(@storage, @shared_read_cache, lock_handler, @commit_lock)
	  r = yield(tx)
	}
      else
	if (@read_only) then
	  raise 'not writable'
	end
	@lock_manager.transaction(false) {|lock_handler|
	  tx = ReadWriteTransactionContext.new(@storage, @shared_read_cache, lock_handler, @commit_lock)
	  r = yield(tx)
	  tx.commit
	}
      end
      r
    end

    class ReadTransactionContext
      extend Forwardable
      include Enumerable

      def initialize(storage, read_cache, lock_handler, commit_lock)
	@tx = storage.class::TransactionContext.new(storage, read_cache, lock_handler)
	@storage = storage
	@commit_lock = commit_lock
      end

      def_delegator :@tx, :locked?
      def_delegator :@tx, :lock
      def_delegator :@tx, :unlock
      def_delegator :@tx, :[]

      def_delegator :@tx, :key?
      alias has_key? key?
      alias include? key?

      def_delegator :@tx, :property
      def_delegator :@tx, :each_property
      def_delegator :@tx, :property?
      alias has_property? property?

      def each_key
	@tx.each_key do |key|
	  yield(key)
	end
	self
      end

      def each_value
	each_key do |key|
	  yield(self[key])
	end
      end

      def each_pair
	each_key do |key|
	  yield(key, self[key])
	end
      end

      alias each each_pair

      def keys
	key_list = []
	each_key do |key|
	  key_list << key
	end
	key_list
      end

      def values
	value_list = []
	each_value do |value|
	  value_list << value
	end
	value_list
      end

      def length
	len = 0
	each_key do |key|
	  len += 1
	end
	len
      end

      alias size length

      def empty?
	length == 0
      end
    end

    class ReadWriteTransactionContext < ReadTransactionContext
      def_delegator :@tx, :[]=
      def_delegator :@tx, :delete

      def delete_if(*keys)
	del_list = []
	if (keys.empty?) then
	  each_key do |key|
	    if (yield(key, self[key])) then
	      del_list << key
	    end
	  end
	else
	  for key in keys
	    if (@tx.key? key) then
	      if (yield(key, self[key])) then
		del_list << key
	      end
	    end
	  end
	end
	for key in del_list
	  @tx.delete(key)
	end
	self
      end

      def clear
	for key in self.keys
	  @tx.delete(key)
	end
	self
      end

      def commit
	write_list = @tx.write_list
	unless (write_list.empty?) then
	  @storage.write_and_commit(write_list)
	  @tx.write_clear
	end
	nil
      end

      def_delegator :@tx, :rollback
    end
  end
end
