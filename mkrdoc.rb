#!/usr/local/bin/ruby

CVS_ID = '$Id$'			# for ident(1)

require 'yaml'

rdoc_opts = YAML.load(IO.read('rdoc.yml'))
system 'rdoc', *(rdoc_opts['CommonOptions'] + rdoc_opts['CommandLineOptions']).flatten
