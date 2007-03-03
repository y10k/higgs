# $Id$

module Higgs
  module Exceptions
    # for ident(1)
    CVS_ID = '$Id$'

    class HiggsError < StandardError
    end

    class HiggsException < Exception
    end

    class ShutdownException < HiggsException
    end
  end
  include Exceptions
end