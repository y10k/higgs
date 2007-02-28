# $Id$

require 'thread'
require 'forwardable'

module Higgs
  class DBM
    # for ident(1)
    CVS_ID = '$Id$'

    module InitOptions
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

        if (options.include? :cache_type) then
          @cache_type = options[:cache_type]
        else
          require 'higgs/cache'
          @cache_type = Cache::SharedWorkCache
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

    def initialize(name, options={})
      @name = name
      init_options(options)
      @storage = @storage_type.new(name, options)
      @read_cache = @cache_type.new{|key|
        @storage.fetch(key)
      }
      @commit_lock = Mutex.new
    end

    def transaction(read_only=false)
      r = nil
      if (read_only) then
	@lock_manager.transaction(true) {|lock_handler|
	  tx = ReadTransactionContext.new(@storage, @read_cache, lock_handler, @commit_lock)
	  r = yield(tx)
	}
      else
	@lock_manager.transaction(false) {|lock_handler|
	  tx = ReadWriteTransactionContext.new(@storage, @read_cache, lock_handler, @commit_lock)
	  r = yield(tx)
	  tx.commit
	}
      end
      r
    end

    class ReadTransactionContext
      extend Forwardable

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
	  yield(self, self[key])
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
	  len += 0
	end
	len
      end

      alias size length

      def empty?
	length > 0
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
