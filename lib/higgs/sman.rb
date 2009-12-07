# -*- coding: utf-8 -*-
# = storage manager
# == license
#   :include:../LICENSE
#

require 'forwardable'
require 'higgs/services'
require 'higgs/storage'
require 'higgs/tman'

module Higgs
  # = storage manager
  # the front end of these classes.
  # * Higgs::Storage
  # * Higgs::TransactionManager
  # * Higgs::RemoteServices
  #
  # these methods are delegated to Higgs::Storage.
  # * Higgs::Storage#name
  # * Higgs::Storage#number_of_read_io
  # * Higgs::Storage#data_hash_type
  # * Higgs::Storage#jlog_sync
  # * Higgs::Storage#jlog_hash_type
  # * Higgs::Storage#jlog_rotate_size
  # * Higgs::Storage#jlog_rotate_max
  # * Higgs::Storage#shutdown?
  # * Higgs::Storage#alive?
  # * Higgs::Storage#change_number
  # * Higgs::Storage#rotate_journal_log
  #
  # these methods are delegated to Higgs::TransactionManager.
  # * Higgs::TransactionManager#read_only
  # * Higgs::TransactionManager#transaction
  # * Higgs::TransactionManager#apply_journal_log
  # * Higgs::TransactionManager#switch_to_write
  #
  # these methods are delegated to Higgs::RemoteServices.
  # * Higgs::RemoteServices#remote_services_uri
  #
  class StorageManager
    extend Forwardable

    # <tt>name</tt> is a storage name and see Higgs::Storage.new for
    # detail. see Higgs::Storage::InitOptions,
    # Higgs::TransactionManager::InitOptions and
    # Higgs::RemoteServices.new for <tt>options</tt>.
    def initialize(name, options={})
      @storage = Storage.new(name, options)
      @tman = TransactionManager.new(@storage, options)
      options = options.dup
      options[:storage] = @storage
      options[:transaction_manager] = @tman
      @services = RemoteServices.new(options)
    end

    def_delegator :@storage, :name
    def_delegator :@storage, :number_of_read_io
    def_delegator :@storage, :data_hash_type
    def_delegator :@storage, :jlog_sync
    def_delegator :@storage, :jlog_hash_type
    def_delegator :@storage, :jlog_rotate_size
    def_delegator :@storage, :jlog_rotate_max
    def_delegator :@storage, :shutdown?
    def_delegator :@storage, :alive?
    def_delegator :@storage, :change_number
    def_delegator :@storage, :rotate_journal_log
    def_delegator :@tman, :read_only
    def_delegator :@tman, :transaction
    def_delegator :@tman, :apply_journal_log
    def_delegator :@tman, :switch_to_write
    def_delegator :@services, :remote_services_uri

    def shutdown
      @storage.shutdown
      @services.shutdown
      nil
    end

    def self.open(*args)
      sman = new(*args)
      begin
        r = yield(sman)
      ensure
        sman.shutdown
      end
      r
    end
  end
end

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
