# transaction manager
#
# Author:: $Author$
# Date:: $Date$
# Revision:: $Revision$
#

require 'forwardable'
require 'higgs/cache'
require 'higgs/exceptions'
require 'higgs/lock'
require 'higgs/storage'
require 'singleton'

module Higgs
  class TransactionManager
    # for ident(1)
    CVS_ID = '$Id$'

    include Exceptions

    class Error < HiggsError
    end

    class NotWritableError < Error
    end

    class PseudoSecondaryCache
      include Singleton

      def [](key)
      end

      def []=(key, value)
        value
      end

      def delete(key)
      end
    end

    module InitOptions
      # these options are defined.
      # [<tt>:read_only</tt>] if <tt>true</tt> then transaction is always read-only.
      #                       default is <tt>false</tt>.
      # [<tt>:decode</tt>] procedure to decode data on read. if <tt>:string_only</tt>
      #                    property at an entry is <tt>true</tt> then <tt>decode</tt>
      #                    is not used to read the entry. default is <tt>proc{|r| r }</tt>.
      # [<tt>:encode</tt>] procedure to encode data on write. if <tt>:string_only</tt>
      #                    property at an entry is <tt>true</tt> then <tt>encode</tt>
      #                    is not used to write the entry. default is <tt>proc{|w| w }</tt>.
      # [<tt>:lock_manager</tt>] lock of a transaction and individual data. default is
      #                          a new instance of Higgs::GiantLockManager.
      # [<tt>:master_cache</tt>] read-cache for encoded string data. defauilt is
      #                          a new instance of Higgs::LRUCache.
      # [<tt>:secondary_cache</tt>] secondary read-cache for encoded string data.
      #                             key of cache is always unique string.
      #                             default is no effect.
      def init_options(options)
        if (options.key? :read_only) then
          @read_only = options[:read_only]
        else
          @read_only = false
        end

        @decode = options[:decode] || proc{|r| r }
        @encode = options[:encode] || proc{|w| w }
        @lock_manager = options[:lock_manager] || GiantLockManager.new
        @master_cache = options[:master_cache] || LRUCache.new
        @secondary_cache = options[:secondary_cache] || PseudoSecondaryCache.instance
      end

      attr_reader :read_only
    end
    include InitOptions

    # export transaction manager methods from <tt>@tman</tt> instance variable.
    #
    # these methods are delegated.
    # * Higgs::TransactionManager#read_only
    # * Higgs::TransactionManager#transaction
    #
    module Export
      extend Forwardable

      def_delegator :@tman, :read_only
      def_delegator :@tman, :transaction
    end

    def initialize(storage, options={})
      @storage = storage
      init_options(options)
      @master_cache = SharedWorkCache.new(@master_cache) {|key|
        (id = @storage.identity(key) and @secondary_cache[id]) or
          (value = @storage.fetch(key) and @secondary_cache[@storage.identity(key)] = value.freeze)
      }
    end

    # <tt>tx</tt> of block argument is transaction context and see
    # Higgs::TransactionContext for detail.
    def transaction(read_only=@read_only)
      r = nil
      @lock_manager.transaction(read_only) {|lock_handler|
        begin
          if (TransactionManager.in_transaction?) then
            raise 'nested transaction forbidden'
          end
          if (read_only) then
            tx = ReadOnlyTransactionContext.new(lock_handler, @storage, @master_cache, @secondary_cache, @decode, @encode)
          else
            if (@read_only) then
              raise NotWritableError, 'not writable'
            end
            tx = ReadWriteTransactionContext.new(lock_handler, @storage, @master_cache, @secondary_cache, @decode, @encode)
          end
          Thread.current[:higgs_current_transaction] = tx
          r = yield(tx)
          tx.commit unless read_only
        ensure
          Thread.current[:higgs_current_transaction] = nil
        end
      }
      r
    end

    def self.in_transaction?
      ! Thread.current[:higgs_current_transaction].nil?
    end

    def self.current_transaction
      Thread.current[:higgs_current_transaction]
    end
  end

  class TransactionContext
    # for ident(1)
    CVS_ID = '$Id$'

    include Enumerable

    def deep_copy(obj)
      Marshal.load(Marshal.dump(obj))
    end
    private :deep_copy

    def initialize(lock_handler, storage, master_cache, secondary_cache, decode, encode)
      @lock_handler = lock_handler
      @storage = storage
      @master_cache = master_cache
      @secondary_cache = secondary_cache
      @decode = decode
      @encode = encode

      @local_data_cache = Hash.new{|hash, key|
        if (@storage.key? key) then
          if (@storage.string_only(key)) then
            hash[key] = @master_cache[key]
          else
            hash[key] = @decode.call(@master_cache[key])
          end
        end
      }

      @local_properties_cache = Hash.new{|hash, key|
        if (properties = @storage.fetch_properties(key)) then
          hash[key] = deep_copy(properties)
        else
          hash[key] = { 'system_properties' => {}, 'custom_properties' => {} }
        end
      }

      @locked_map = {}
      @locked_map.default = false
      @update_system_properties = {}
      @update_custom_properties = {}
      @ope_map = {}
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
      (@ope_map[key] != :delete) ? @local_data_cache[key] : nil
    end

    def []=(key, value)
      lock(key)
      @ope_map[key] = :write
      @local_data_cache[key] = value
    end

    def update(key, default_value=nil)
      if (self.key? key) then   # lock
        value = self[key]
      else
        unless (default_value) then
          raise IndexError, "not exist properties at key: #{key}"
        end
        value = default_value
      end
      r = yield(value)
      self[key] = value
      r
    end

    def delete(key)
      lock(key)
      if (@ope_map[key] != :delete) then
        @ope_map[key] = :delete
        @update_system_properties.delete(key)
        @update_custom_properties.delete(key)
        @local_properties_cache.delete(key)
        @local_data_cache[key]  # data load from storage
        @local_data_cache.delete(key)
      end
    end

    def key?(key)
      lock(key)
      if (@ope_map[key] != :delete) then
        if (@local_data_cache.key? key) then
          return true
        end
        if (@storage.key? key) then
          return true
        end
      end
      false
    end

    alias has_key? key?
    alias include? key?
    alias root? key?

    def each_key
      @local_data_cache.each_key do |key|
        lock(key)
        if (@ope_map[key] != :delete) then
          yield(key)
        end
      end
      @storage.each_key do |key|
        lock(key)
        if (@ope_map[key] != :delete) then
          unless (@local_data_cache.key? key) then
            yield(key)
          end
        end
      end
      self
    end

    def each_value              # :yields: value
      each_key do |key|
        yield(self[key])
      end
    end

    def each_pair               # :yields: key, value
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

    alias roots keys

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

    def property(key, name)
      case (name)
      when Symbol, String
        # good
      else
        raise TypeError, "can't convert #{name.class} (name) to Symbol or String"
      end

      lock(key)
      if (@ope_map[key] != :delete) then
        if (properties = @local_properties_cache[key]) then
          case (name)
          when Symbol
            properties['system_properties'][name.to_s]
          when String
            properties['custom_properties'][name]
          end
        end
      end
    end

    def set_property(key, name, value)
      unless (self.key? key) then # lock
        raise IndexError, "not exist properties at key: #{key}"
      end
      case (name)
      when String
        properties = @local_properties_cache[key]['custom_properties']
        properties[name] = value
        @update_custom_properties[key] = properties
      when :string_only
        properties = @local_properties_cache[key]['system_properties']
        properties['string_only'] = value
        @update_system_properties[key] = properties
      else
        raise TypeError, "can't convert #{name.class} (name) to String"
      end
      nil
    end

    def delete_property(key, name)
      unless (self.key? key) then # lock
        raise IndexError, "not exist properties at key: #{key}"
      end
      unless (name.kind_of? String) then
        raise TypeError, "can't convert #{name.class} (name) to String"
      end
      properties = @local_properties_cache[key]['custom_properties']
      if (properties.key? name) then
        value = properties.delete(name)
        @update_custom_properties[key] = properties
        return value
      end
      nil
    end

    def property?(key, name)
      case (name)
      when Symbol, String
        # good
      else
        raise TypeError, "can't convert #{name.class} (name) to Symbol or String"
      end

      if (self.key? key) then   # lock
        case (name)
        when Symbol
          return (@local_properties_cache[key]['system_properties'].key? name.to_s)
        when String
          return (@local_properties_cache[key]['custom_properties'].key? name)
        else
          raise 'Bug: not to reach'
        end
      end
      false
    end

    def each_property(key)      # :yields: name, value
      unless (self.key? key) then # lock
        raise IndexError, "not exist properties at key: #{key}"
      end
      @local_properties_cache[key]['system_properties'].each_pair do |name, value|
        yield(name.to_sym, value)
      end
      @local_properties_cache[key]['custom_properties'].each_pair do |name, value|
        yield(name, value)
      end
      self
    end

    def delete_if(*keys)        # :yields: key, value
      del_list = []
      if (keys.empty?) then
        each_key do |key|
          if (yield(key, self[key])) then
            del_list << key
          end
        end
      else
        for key in keys
          if (self.key? key) then
            if (yield(key, self[key])) then
              del_list << key
            end
          end
        end
      end
      for key in del_list
        delete(key)
      end
      nil
    end

    def clear
      for key in keys
        delete(key)
      end
      nil
    end

    def write_list
      @ope_map.map{|key, ope|
        if (ope == :delete) then
          [ ope, key ]
        else
          if (@local_properties_cache[key]['system_properties']['string_only']) then
            [ ope, key, @local_data_cache[key] ]
          else
            [ ope, key, @encode.call(@local_data_cache[key]) ]
          end
        end
      } + \
      @update_system_properties.map{|key, properties|
        [ :system_properties, key, properties ]
      } + \
      @update_custom_properties.map{|key, properties|
        [ :custom_properties, key, properties ]
      }
    end
    private :write_list

    def commit
      write_list = write_list()
      unless (write_list.empty?) then
        @storage.write_and_commit(write_list)
        for ope, key, value in write_list
          case (ope)
          when :write
            @master_cache.delete(key)
            @secondary_cache[key] = value
          when :delete
            @master_cache.delete(key)
            @secondary_cache.delete(key)
          end
          @local_properties_cache.delete(key)
        end
        @update_system_properties.clear
        @update_custom_properties.clear
        @ope_map.clear
      end
      nil
    end

    def rollback
      @local_data_cache.clear
      @local_properties_cache.clear
      @update_system_properties.clear
      @update_custom_properties.clear
      @ope_map.clear
      nil
    end
  end

  class ReadOnlyTransactionContext < TransactionContext
    undef []=
    undef update
    undef delete
    undef set_property
    undef delete_property
    undef delete_if
    undef clear
    undef commit
    undef rollback
  end

  class ReadWriteTransactionContext < TransactionContext
  end
end

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
