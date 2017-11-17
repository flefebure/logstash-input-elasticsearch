# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "base64"
require "yaml"

# .Compatibility Note
# [NOTE]
# ================================================================================
# Starting with Elasticsearch 5.3, there's an {ref}modules-http.html[HTTP setting]
# called `http.content_type.required`. If this option is set to `true`, and you
# are using Logstash 2.4 through 5.2, you need to update the Elasticsearch input
# plugin to version 4.0.2 or higher.
#
# ================================================================================
#
# Read from an Elasticsearch cluster, based on search query results.
# This is useful for replaying test logs, reindexing, etc.
#
# Example:
# [source,ruby]
#     input {
#       # Read all documents from Elasticsearch matching the given query
#       elasticsearch {
#         hosts => "localhost"
#         query => '{ "query": { "match": { "statuscode": 200 } }, "sort": [ "_doc" ] }'
#       }
#     }
#
# This would create an Elasticsearch query with the following format:
# [source,json]
#     curl 'http://localhost:9200/logstash-*/_search?&scroll=1m&size=1000' -d '{
#       "query": {
#         "match": {
#           "statuscode": 200
#         }
#       },
#       "sort": [ "_doc" ]
#     }'
#
class LogStash::Inputs::Elasticsearch < LogStash::Inputs::Base
  config_name "elasticsearch"

  default :codec, "json"

  # List of elasticsearch hosts to use for querying.
  # Each host can be either IP, HOST, IP:port or HOST:port.
  # Port defaults to 9200
  config :hosts, :validate => :array

  # The index or alias to search.
  config :index, :validate => :string, :default => "logstash-*"

  # The query to be executed. Read the Elasticsearch query DSL documentation
  # for more info
  # https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl.html
  config :query, :validate => :string, :default => '{ "sort": [ "_doc" ] }'

  # This allows you to set the maximum number of hits returned per scroll.
  config :size, :validate => :number, :default => 1000

  # This parameter controls the keepalive time in seconds of the scrolling
  # request and initiates the scrolling process. The timeout applies per
  # round trip (i.e. between the previous scroll request, to the next).
  config :scroll, :validate => :string, :default => "1m"

  config :legacy_scrolling, :validate => :boolean, :default => false

  # If set, include Elasticsearch document information such as index, type, and
  # the id in the event.
  #
  # It might be important to note, with regards to metadata, that if you're
  # ingesting documents with the intent to re-index them (or just update them)
  # that the `action` option in the elasticsearch output wants to know how to
  # handle those things. It can be dynamically assigned with a field
  # added to the metadata.
  #
  # Example
  # [source, ruby]
  #     input {
  #       elasticsearch {
  #         hosts => "es.production.mysite.org"
  #         index => "mydata-2018.09.*"
  #         query => "*"
  #         size => 500
  #         scroll => "5m"
  #         docinfo => true
  #       }
  #     }
  #     output {
  #       elasticsearch {
  #         index => "copy-of-production.%{[@metadata][_index]}"
  #         document_type => "%{[@metadata][_type]}"
  #         document_id => "%{[@metadata][_id]}"
  #       }
  #     }
  #
  config :docinfo, :validate => :boolean, :default => false

  # Where to move the Elasticsearch document information. By default we use the @metadata field.
  config :docinfo_target, :validate=> :string, :default => LogStash::Event::METADATA

  # List of document metadata to move to the `docinfo_target` field.
  # To learn more about Elasticsearch metadata fields read
  # http://www.elasticsearch.org/guide/en/elasticsearch/guide/current/_document_metadata.html
  config :docinfo_fields, :validate => :array, :default => ['_index', '_type', '_id']

  # Basic Auth - username
  config :user, :validate => :string

  # Basic Auth - password
  config :password, :validate => :password

  # SSL
  config :ssl, :validate => :boolean, :default => false

  # SSL Certificate Authority file in PEM encoded format, must also include any chain certificates as necessary
  config :ca_file, :validate => :path

  # Path to file with last run time
  config :last_run_metadata_path, :validate => :string, :default => "#{ENV['HOME']}/.logstash_elastic_last_run"
  # If tracking column value rather than timestamp, the column whose value is to be tracked
  config :tracking_field, :validate => :string
  # Type of tracking column. Currently only "numeric" and "timestamp"
  config :tracking_field_type, :validate => ['numeric', 'timestamp'], :default => 'numeric'
  # Whether the previous run state should be preserved
  config :clean_run, :validate => :boolean, :default => false
  # Whether to save state or not in last_run_metadata_path
  config :record_last_run, :validate => :boolean, :default => false

  # Schedule of when to periodically run statement, in Cron format
  # for example: "* * * * *" (execute query every minute, on the minute)
  #
  # There is no schedule by default. If no schedule is given, then the statement is run
  # exactly once.
  config :schedule, :validate => :string

  def register
    require "elasticsearch"
    require "rufus/scheduler"

    @options = {
      :index => @index,
      :body => @query,
      :scroll => @scroll,
      :size => @size
    }

    # Raise an error if @use_column_value is true, but no @tracking_column is set
    if @use_field_value
      if @tracking_field.nil?
        raise(LogStash::ConfigurationError, "Must set :tracking_field if :use_field_value is true.")
      end
    end

    # load sql_last_value from file if exists
    @elastic_last_value = 0
    @elastic_last_page_value = -1
    if @clean_run && File.exist?(@last_run_metadata_path)
      File.delete(@last_run_metadata_path)
    elsif File.exist?(@last_run_metadata_path)
      @elastic_last_value = YAML.load(File.read(@last_run_metadata_path))
      logger.info("using elastic_last_value #{@elastic_last_value}")
    end


    transport_options = {}

    if @user && @password
      token = Base64.strict_encode64("#{@user}:#{@password.value}")
      transport_options[:headers] = { :Authorization => "Basic #{token}" }
    end

    hosts = if @ssl then
      @hosts.map do |h|
        host, port = h.split(":")
        { :host => host, :scheme => 'https', :port => port }
      end
    else
      @hosts
    end

    if @ssl && @ca_file
      transport_options[:ssl] = { :ca_file => @ca_file }
    end

    @client = Elasticsearch::Client.new(:hosts => hosts, :transport_options => transport_options)

  end

  def update_state_file

    if @record_last_run
      File.write(@last_run_metadata_path, YAML.dump(@elastic_last_value))
      if @elastic_last_value == @elastic_last_page_value
        false
      else
        @elastic_last_page_value = @elastic_last_value
        true
      end
    else
      true
    end
  end


  def execute_query(output_queue)
    # get first wave of data
    if @record_last_run
      updatedquery = @query.sub("&elastic_last_value", @elastic_last_value.to_s)
      @options = {
          :index => @index,
          :body => updatedquery,
          :scroll => @scroll,
          :size => @size
      }

    end
    r = @client.search(@options)

    r['hits']['hits'].each { |hit| push_hit(hit, output_queue) }
    has_hits = r['hits']['hits'].any?
    update_state_file

    while has_hits && !stop?
      r = process_next_scroll(output_queue, r['_scroll_id'])
      u = update_state_file
      has_hits = u && r['has_hits']
    end
  end

  def run(output_queue)
   if @schedule
      @scheduler = Rufus::Scheduler.new(:max_work_threads => 1)
      @scheduler.cron @schedule do
        execute_query(output_queue)
      end

      @scheduler.join
    else
      execute_query(output_queue)
    end
  end

  private

  def process_next_scroll(output_queue, scroll_id)
    @logger.info("process new scroll")
    r = scroll_request(scroll_id)
    r['hits']['hits'].each { |hit| push_hit(hit, output_queue) }
    {'has_hits' => r['hits']['hits'].any?, '_scroll_id' => r['_scroll_id']}
  end

  def push_hit(hit, output_queue)
    event = LogStash::Event.new(hit['_source'])
    decorate(event)

    if @record_last_run
      if !hit['_source'][tracking_field].nil? && hit['_source'][tracking_field] >  @elastic_last_value
         @elastic_last_value = hit['_source'][tracking_field]
      end
    end

    if @docinfo
      # do not assume event[@docinfo_target] to be in-place updatable. first get it, update it, then at the end set it in the event.
      docinfo_target = event.get(@docinfo_target) || {}

      unless docinfo_target.is_a?(Hash)
        @logger.error("Elasticsearch Input: Incompatible Event, incompatible type for the docinfo_target=#{@docinfo_target} field in the `_source` document, expected a hash got:", :docinfo_target_type => docinfo_target.class, :event => event)

        # TODO: (colin) I am not sure raising is a good strategy here?
        raise Exception.new("Elasticsearch input: incompatible event")
      end

      @docinfo_fields.each do |field|
        docinfo_target[field] = hit[field]
      end

      event.set(@docinfo_target, docinfo_target)
    end

    output_queue << event
  end

  def scroll_request scroll_id
    if @legacy_scrolling
      @client.scroll(:body => scroll_id , :scroll => @scroll)
    else
      @client.scroll(:body => { :scroll_id => scroll_id }, :scroll => @scroll)
    end
  end
end
