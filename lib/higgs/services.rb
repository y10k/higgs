# -*- coding: utf-8 -*-

require 'drb'

module Higgs
  # = remote services
  class RemoteServices
    # theses options are defined.
    # [<tt>:remote_services_uri</tt>] URI for DRb remote call to provide services.
    # [<tt>:storage</tt>] an instance of Higgs::Storage as remote service provider.
    # [<tt>:transaction_manager</tt>] an instance of Higgs::TransactionManager
    #                                 as remote service provider.
    #
    # these methods are exported as remote services.
    # * Higgs::Storage#alive?
    # * Higgs::Storage#localhost_check
    # * Higgs::Storage#rotate_journal_log
    # * Higgs::TransactionManager#apply_journal_log
    # * Higgs::TransactionManager#switch_to_write
    #
    def initialize(options)
      @remote_services_uri = options[:remote_services_uri]
      @storage = options[:storage]
      @tman = options[:transaction_manager]

      @service_map = {}
      if (@storage) then
        @service_map[:alive_service_v1] = @storage.method(:alive?)
        @service_map[:localhost_check_service_v1] = @storage.method(:localhost_check)
        @service_map[:jlog_rotate_service_v1] = @storage.method(:rotate_journal_log)
      end
      if (@tman) then
        @service_map[:jlog_apply_service_v1] = @tman.method(:apply_journal_log)
        @service_map[:switch_to_write_service_v1] = @tman.method(:switch_to_write)
      end

      @service_map.extend(DRb::DRbUndumped)
      @service_map.freeze
      @server = DRb::DRbServer.new(@remote_services_uri, @service_map) if @remote_services_uri
    end

    attr_reader :remote_services_uri

    def shutdown
      @server.stop_service if @remote_services_uri
      nil
    end
  end
end

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
