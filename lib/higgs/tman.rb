# -*- coding: utf-8 -*-

require 'higgs/cache'
require 'higgs/exceptions'
require 'higgs/storage'
require 'singleton'
require 'thread'

module Higgs
  class TransactionManager
    include Exceptions

    class Error < HiggsError
    end

    class NotWritableError < Error
    end

    # options for Higgs::TransactionManager
    module InitOptions
      # these options are defined.
      # [<tt>:read_only</tt>] if <tt>true</tt> then transaction is always read-only.
      #                       default is <tt>false</tt>.
      #                       <tt>:standby</tt> is standby mode. in standby mode, transaction is
      #                       read-only and Higgs::TransactionManager#apply_journal_log is callable.
      #                       if Higgs::TransactionManager#switch_to_write is called in standby mode
      #                       then state of manager changes from standby mode to read-write mode.
      # [<tt>:decode</tt>] procedure to decode data on read. if <tt>:string_only</tt>
      #                    property at an entry is <tt>true</tt> then <tt>decode</tt>
      #                    is not used to read the entry. default is <tt>proc{|r| r }</tt>.
      # [<tt>:encode</tt>] procedure to encode data on write. if <tt>:string_only</tt>
      #                    property at an entry is <tt>true</tt> then <tt>encode</tt>
      #                    is not used to write the entry. default is <tt>proc{|w| w }</tt>.
      # [<tt>:jlog_apply_dir</tt>] journal logs under the directory of this parameter is
      #                            applied to storage on call of
      #                            Higgs::TransactionManager#apply_journal_log
      def init_options(options)
        if (options.key? :read_only) then
          @read_only = options[:read_only]
        else
          @read_only = false
        end

        @decode = options[:decode] || proc{|r| r }
        @encode = options[:encode] || proc{|w| w }
        @jlog_apply_dir = options[:jlog_apply_dir] || nil
      end

      attr_reader :read_only
    end
    include InitOptions

    # see Higgs::TransactionManager::InitOptions for <tt>options</tt>.
    def initialize(storage, options={})
      @storage = storage
      @write_lock = Mutex.new
      init_options(options)
      if (@read_only == :standby && ! @jlog_apply_dir) then
        raise ArgumentError, "need for `:jlog_apply_dir' parameter in standby mode"
      end
    end

    # <tt>tx</tt> of block argument is transaction context and see
    # Higgs::TransactionContext for detail.
    def transaction(read_only=@read_only)
      r = nil
      if (read_only) then
        @storage.transaction(read_only) {|st_hndl|
          tx = ReadOnlyTransactionContext.new(st_hndl, @decode, @encode)
          r = yield(tx)
        }
      else
        if (@read_only) then
          raise NotWritableError, 'not writable'
        end
        @write_lock.synchronize{
          @storage.transaction(read_only) {|st_hndl|
            tx = ReadWriteTransactionContext.new(st_hndl, @decode, @encode)
            r = yield(tx)
            tx.commit
          }
        }
      end

      r
    end

    def apply_journal_log(not_delete=false)
      @write_lock.synchronize{
        if (@read_only != :standby) then
          raise "not standby mode: #{@read_only}"
        end
        name = File.join(@jlog_apply_dir, File.basename(@storage.name))
        for jlog_path in Storage.rotated_entries("#{name}.jlog")
          @storage.apply_journal_log(jlog_path)
          File.unlink(jlog_path) unless not_delete
        end
      }
      nil
    end

    def switch_to_write
      @write_lock.synchronize{
        if (@read_only != :standby) then
          raise "not standby mode: #{@read_only}"
        end
        @read_only = false
        @storage.switch_to_write
      }
      nil
    end
  end

  class TransactionContext
    include Enumerable

    def deep_copy(obj)
      Marshal.load(Marshal.dump(obj))
    end
    private :deep_copy

    def string_only(key)
      properties = @st_hndl.fetch_properties(key) and
        properties['system_properties']['string_only']
    end
    private :string_only

    def initialize(st_hndl, decode, encode)
      @st_hndl = st_hndl
      @decode = decode
      @encode = encode

      @local_data_cache = Hash.new{|hash, key|
        if (@st_hndl.key? key) then
          if (string_only(key)) then
            hash[key] = @st_hndl.fetch_data(key).freeze
          else
            hash[key] = @decode.call(@st_hndl.fetch_data(key).freeze)
          end
        end
      }

      @local_properties_cache = Hash.new{|hash, key|
        if (props = @st_hndl.fetch_properties(key)) then
          hash[key] = deep_copy(props)
        else
          hash[key] = { 'system_properties' => {}, 'custom_properties' => {} }
        end
      }

      @update_system_properties = {}
      @update_custom_properties = {}
      @ope_map = {}
    end

    def change_number
      @st_hndl.change_number
    end

    def [](key)
      (@ope_map[key] != :delete) ? @local_data_cache[key] : nil
    end

    def []=(key, value)
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
      if (@ope_map[key] != :delete) then
        if (@local_data_cache.key? key) then
          return true
        end
        if (@st_hndl.key? key) then
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
        if (@ope_map[key] != :delete) then
          yield(key)
        end
      end
      @st_hndl.each_key do |key|
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

      if (@ope_map[key] != :delete) then
        if (properties = @local_properties_cache[key]) then
          case (name)
          when :data_change_number
            @st_hndl.data_change_number(key)
          when :properties_change_number
            @st_hndl.properties_change_number(key)
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
        when :data_change_number
          return @st_hndl.data_change_number(key) != nil
        when :properties_change_number
          return @st_hndl.properties_change_number(key) != nil
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
      if (value = @st_hndl.data_change_number(key)) then
        yield(:data_change_number, value)
      end
      if (value = @st_hndl.properties_change_number(key)) then
        yield(:properties_change_number, value)
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
      if (write_list.empty?) then
        return
      end

      for ope, key, value in write_list
        @local_properties_cache.delete(key)
      end
      @update_system_properties.clear
      @update_custom_properties.clear
      @ope_map.clear

      @st_hndl.write_and_commit(write_list)

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
