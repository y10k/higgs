# $Id$

require 'higgs/cache'
require 'higgs/exceptions'
require 'higgs/storage'

module Higgs
  class TransactionManager
    # for ident(1)
    CVS_ID = '$Id$'

    include Exceptions

    class Error < HiggsError
    end

    class NotWritableError < Error
    end

    module InitOptions
      def init_options(options)
        if (options.include? :read_only) then
          @read_only = options[:read_only]
        else
          @read_only = false
        end

        @decode = options[:decode] || proc{|r| r }
        @encode = options[:encode] || proc{|w| w }

        if (options.include? :lock_manager) then
          @lock_manager = options[:lock_manager]
        else
          require 'higgs/lock'
          @lock_manager = GiantLockManager.new
        end

        if (options.include? :master_cache) then
          @master_cache = options[:master_cache]
        else
          @master_cache = LRUCache.new
        end
      end

      attr_reader :read_only
    end
    include InitOptions

    def initialize(storage, options={})
      @storage = storage
      init_options(options)

      @master_cache = SharedWorkCache.new(@master_cache) {|key|
        value = @storage.fetch(key) and @decode.call(value)
      }
    end

    def transaction(read_only=@read_only)
      r = nil
      @lock_manager.transaction(read_only) {|lock_handler|
        if (read_only) then
          tx = ReadOnlyTransactionContext.new(lock_handler, @storage, @master_cache, @decode, @encode)
        else
          if (@read_only) then
            raise NotWritableError, 'not writable'
          end
          tx = ReadWriteTransactionContext.new(lock_handler, @storage, @master_cache, @decode, @encode)
        end
        r = yield(tx)
        tx.commit unless read_only
      }
      r
    end
  end

  class TransactionContext
    # for ident(1)
    CVS_ID = '$Id$'

    include Enumerable

    def initialize(lock_handler, storage, master_cache, decode, encode)
      @lock_handler = lock_handler
      @storage = storage
      @master_cache = master_cache
      @decode = decode
      @encode = encode

      @local_data_cache = Hash.new{|hash, key|
        hash[key] = @master_cache[key] if (@storage.key? key)
      }
      @local_properties_cache = Hash.new{|hash, key|
        if (properties = @storage.fetch_properties(key)) then
          hash[key] = Marshal.load(Marshal.dump(properties)) # deep copy
        else
          hash[key] = { 'system_properties' => {}, 'custom_properties' => {} }
        end
      }

      @locked_map = {}
      @locked_map.default = false
      @update_properties = {}
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

    def delete(key)
      lock(key)
      if (@ope_map[key] != :delete) then
        @ope_map[key] = :delete
        @update_properties.delete(key)
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
          if (@local_data_cache[key] != nil) then
            yield(key)
          end
        end
      end
      @storage.each_key do |key|
        lock(key)
        if (@ope_map[key] != :delete) then
          if (! (@local_data_cache.key? key)) then
            yield(key)
          end
        end
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
      unless (name.kind_of? String) then
        raise TypeError, "can't convert #{name.class} (name) to String"
      end
      unless (self.key? key) then
        raise IndexError, "not exist properties at key: #{key}"
      end
      properties = @local_properties_cache[key]['custom_properties']
      properties[name] = value
      @update_properties[key] = properties
      nil
    end

    def delete_property(key, name)
      unless (name.kind_of? String) then
        raise TypeError, "can't convert #{name.class} (name) to String"
      end
      unless (self.key? key) then
        raise IndexError, "not exist properties at key: #{key}"
      end
      properties = @local_properties_cache[key]['custom_properties']
      if (properties.include? name) then
        value = properties.delete(name)
        @update_properties[key] = properties
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

      if (self.key? key) then
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

    def each_property(key)
      unless (self.key? key) then
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
          [ ope, key, @encode.call(@local_data_cache[key]) ]
        end
      } + \
      @update_properties.map{|key, properties|
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
          when :write, :delete
            @master_cache.delete(key)
          end
          @local_properties_cache.delete(key)
        end
        @update_properties.clear
        @ope_map.clear
      end
      nil
    end

    def rollback
      @local_data_cache.clear
      @local_properties_cache.clear
      @update_properties.clear
      @ope_map.clear
      nil
    end
  end

  class ReadOnlyTransactionContext < TransactionContext
    undef []=
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
