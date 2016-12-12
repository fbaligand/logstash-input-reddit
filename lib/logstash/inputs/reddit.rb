# encoding: utf-8
require 'logstash/inputs/base'
require 'logstash/namespace'
require 'stud/interval'
require 'socket'
require 'net/https'
require 'uri'

class LogStash::Inputs::Reddit < LogStash::Inputs::Base

  config_name 'reddit'
  default :codec, 'json'
  config :subreddit, :validate => :string, :default => 'elastic'
  config :interval, :validate => :number, :default => 10
  config :target, :validate => :string

  public
  def register
    @host = Socket.gethostname
    @http = Net::HTTP.new('www.reddit.com', 443)
    @get = Net::HTTP::Get.new("/r/#{@subreddit}/.json")
    @http.use_ssl = true
  end

  def run(queue)
    # we can abort the loop if stop? becomes true
    while !stop?
      response = @http.request(@get)
      @codec.decode(response.body) do |decoded|
        json = decoded.to_hash
        json['data']['children'].each do |data|
          event = @target ? LogStash::Event.new(@target => data, 'host' => @host) : LogStash::Event.new(data)
          decorate(event)
          queue << event
        end
      end
      Stud.stoppable_sleep(@interval) { stop? }
    end
  end
end
