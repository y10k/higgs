# -*- coding: utf-8 -*-

require 'forwardable'
require 'higgs/block'
require 'higgs/thread'
require 'thread'

module Higgs
  # = storage index
  class Index
    extend Forwardable
    include Block
    include Synchronized

    MAGIC_SYMBOL = 'HIGGS_INDEX'
    MAJOR_VERSION = 0
    MINOR_VERSION = 2

    def initialize
      @change_number = 0
      @eoa = 0
      @free_lists = {}
      @index = {}
      @identities = {}
      @storage_id = nil
      self.__lock__ = Mutex.new
    end

    synchronized_reader :change_number
    synchronized_accessor :eoa
    synchronized_accessor :storage_id

    def succ!
      @change_number = @change_number.succ
      self
    end
    synchronized :succ!

    def free_fetch(size)
      @free_lists[size].shift if (@free_lists.key? size)
    end
    synchronized :free_fetch

    def free_fetch_at(pos, size)
      @free_lists[size].delete(pos) or raise "not found free region: (#{pos},#{size})"
    end
    synchronized :free_fetch_at

    def free_store(pos, size)
      @free_lists[size] = [] unless (@free_lists.key? size)
      @free_lists[size] << pos
      nil
    end
    synchronized :free_store

    def_synchronized_delegator :@index, :key?
    def_synchronized_delegator :@index, :keys

    def identity(key)
      i = @index[key] and i[0]
    end
    synchronized :identity

    def [](key)
      i = @index[key] and i[1]
    end
    synchronized :[]

    def self.create_id(key, identities)
      id = key.to_s
      if (identities.key? id) then
        id += '.a'
        id.succ! while (identities.key? id)
      end
      id
    end

    def []=(key, value)
      if (i = @index[key]) then
        i[1] = value
      else
        id = Index.create_id(key, @identities)
        @identities[id] = key
        @index[key] = [ id, value ]
      end
      value
    end
    synchronized :[]=

    def delete(key)
      id, value = @index.delete(key)
      @identities.delete(id) if id
      value
    end
    synchronized :delete

    def each_key
      keys = __lock__.synchronize{ @index.keys }
      for key in keys
        yield(key)
      end
      self
    end

    def to_h
      { :version => [ MAJOR_VERSION, MINOR_VERSION ],
        :change_number => @change_number,
        :eoa => @eoa,
        :free_lists => @free_lists,
        :index => @index,
        :identities => @identities,
        :storage_id => @storage_id
      }
    end
    synchronized :to_h

    def migration_0_0_to_0_1(index_data)
      if ((index_data[:version] <=> [ 0, 0 ]) > 0) then
        return
      end
      if ((index_data[:version] <=> [ 0, 0 ]) < 0) then
        raise "unexpected index format version: #{index_data[:version].join('.')}"
      end

      index = index_data[:index]
      identities = index_data[:identities] = {}
      for key in index.keys
        id = Index.create_id(key, identities)
        identities[id] = key
        value = index[key]
        index[key] = [ id, value ]
      end
      index_data[:version] = [ 0, 1 ]

      index_data
    end
    private :migration_0_0_to_0_1

    def migration_0_1_to_0_2(index_data)
      if ((index_data[:version] <=> [ 0, 1 ]) > 0) then
        return
      end
      if ((index_data[:version] <=> [ 0, 1 ]) < 0) then
        raise "unexpected index format version: #{index_data[:version].join('.')}"
      end

      index_data[:storage_id] = @storage_id
      index_data[:version] = [ 0, 2 ]

      index_data
    end
    private :migration_0_1_to_0_2

    def save(path)
      tmp_path = "#{path}.tmp.#{$$}"
      File.open(tmp_path, File::WRONLY | File::CREAT | File::TRUNC, 0660) {|f|
        f.binmode
        f.set_encoding(Encoding::ASCII_8BIT)
        block_write(f, MAGIC_SYMBOL,
                    __lock__.synchronize{
                      Marshal.dump(thread_unsafe_to_h)
                    })
        f.fsync
      }
      File.rename(tmp_path, path)
      self
    end

    def load(path)
      File.open(path, 'r') {|f|
        f.binmode
        f.set_encoding(Encoding::ASCII_8BIT)
        index_data = Marshal.load(block_read(f, MAGIC_SYMBOL))
        migration_0_0_to_0_1(index_data)
        migration_0_1_to_0_2(index_data)
        if (index_data[:version] != [ MAJOR_VERSION, MINOR_VERSION ]) then
          raise "unsupported version: #{index_data[:version].join('.')}"
        end
        __lock__.synchronize{
          @change_number = index_data[:change_number]
          @eoa = index_data[:eoa]
          @free_lists = index_data[:free_lists]
          @index = index_data[:index]
          @identities = index_data[:identities]
          @storage_id = index_data[:storage_id]
        }
      }
      self
    end
  end
end

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
