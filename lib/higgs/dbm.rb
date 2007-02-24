# $Id$

require 'thread'

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
          @storage_type = Higgs::Storage
        end

        if (options.include? :cache_type) then
          @cache_type = options[:cache_type]
        else
          require 'higgs/cache'
          @cache_type = Higgs::Cache::SharedWorkCache
        end

        if (options.include? :lock_manager) then
          @lock_manager = options[:lock_manager]
        else
          require 'higgs/lock'
          @lock_manager = Higgs::Lock::FineGrainLockManager.new
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

    class TransactionHandler
      include Enumerable

      def initialize(storage, read_cache, lock_handler)
        @storage = storage
        @local_cache = Hash.new{|hash, key| hash[key] = read_cache[key] }
        @lock_handler = lock_handler
        @locked_map = {}
        @locked_map.default = false
        @deleted_map = {}
        @deleted_map.default = false
        @immediate_release = false
        @write_list = []
      end

      attr_accessor :immediate_release
      attr_reader :write_list

      def locked?(key)
        @locked_map[key]
      end

      def lock(key)
        unless (@locked_map[key]) then
          @lock_handler.lock(key)
          @locked_map[key] = true
          return true
        end
        false
      end

      def unlock(key)
        if (@locked_map[key]) then
          @lock_handler.unlock(key)
          @locked_map[key] = false
          return true
        end
        false
      end
    end

    module ReadTransaction
      def [](key)
        lock(key)
        ! @deleted_map[key] ? @local_cache[key] : nil
      end

      def each_key
        @storage.each_key do |key|
          locked = locked_map[key]
          lock(key) unless locked
          yield(key) unless @deleted_map[key]
          unlock(key) if (locked && immediate_release)
        end
        self
      end

      def each_value
        each_key do |key|
          yield(@local_cache[key])
        end
      end

      def each_pair
        each_key do |key|
          yield(key, @local_cache[key])
        end
      end

      alias each each_pair

      def key?(key)
        lock(key)
        (! @deleted_map[key]) && (@storage.key? key)
      end

      alias has_key? key?
      alias include? key?
      alias member? key?

      def keys
        key_list = []
        each_key do |key|
          key_list << key
        end
        key_list
      end

      def values
        value_list = []
        each_key do |key|
          value_list << @local_cache[key]
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

    module WriteTransaction
      def []=(key, value)
        lock(key)
        @deleted_map[key] = false
        @write_list << [ key, :write, value ]
        @local_cache[key] = value
      end

      def delete(key)
        lock(key)
        @write_list << [ key, :delete ]
        unless (@deleted_map[key]) then
          @deleted_map[key] = true
          @local_cache[key]     # load from storage
          @local_cache.delete(key)
        end
      end

      def delete_if
        del_key_list = []
        each_pair do |key, value|
          if (yield(key, value)) then
            del_key_list << key
          end
        end
        for key in del_key_list
          @deleted_map[key] = true
          @write_list << [ key, :delete ]
          @local_cache.delete(key)
        end
        self
      end

      alias reject! delete_if
    end

    class ReadOnlyTransactionHandler < TransactionHandler
      include ReadTransaction
    end

    class ReadWriteTransactionHandler < TransactionHandler
      include ReadTransaction
      include WriteTransaction
    end
  end
end
