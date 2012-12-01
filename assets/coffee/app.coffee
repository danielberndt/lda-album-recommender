class Connector 
  
  constructor: (address) ->
    WS = if window['MozWebSocket'] then MozWebSocket else WebSocket
    @socket = new WS address
    @socket.onmessage = @onreceive
    @$msg = $(".msg").show()
    
  onreceive: (event) =>
    data = JSON.parse(event.data)
    if data.done
      window.location = data.results
    else
      if data.message.type == "error"
        $("<li></li>").addClass("error-message").text("an error has occurred: #{data.message.extra}").appendTo(@$msg)
        @$msg.addClass("error")
      else if data.message.done
        $(".msg-#{data.message.type}-#{data.message.page}", @$msg).addClass("done")
      else
        typeString = if data.message.type == "all" then "all your music" else "your current music"
        $("<li></li>").addClass("msg-#{data.message.type}-#{data.message.page}").text("crawling #{typeString} on page #{data.message.page}").appendTo(@$msg)
    
window["Connector"] = Connector

class Initialisor

  constructor: () ->
    $(document).on "submit", "form", Form.submissionHandler
    $(document).on "click", "a.js-inline", InlineLoader.clickHandler
    
class Form
  
  @submissionHandler: (event) ->
    event.preventDefault()
    form = new Form(this)
    form.submit()
  
  constructor: (el) ->
    @el = el
    @$el = $(el)
  
  submit: () -> 
      $(".error", @$el).remove()
      $.ajax 
        type: @$el.attr("method")
        url: @$el.attr("action")
        data: @$el.serialize()
        dataType: "json"
        success: @onSuccess
        error: @onError
      @$el.addClass("pending")
  
  onSuccess: (data) =>
    @$el.removeClass("pending")
    if window['WebSocket'] or window['MozWebSocket']
      new Connector(data.ws_update)
    else
      $(".msg").show().html("<li>Loading...</li><li><small>PS: you really should get a modern browser that supports web sockets!</small></li>")
      $.ajax
        type: "get"
        url: String data.update
        dataType: "json"
        success: @onUpdateResponse
        
  onError: (xhr) =>
    @$el.removeClass("pending")
    $("<div></div>").text(JSON.parse(xhr.response).error).addClass("error").appendTo(@$el)
      
  onUpdateResponse: (data) =>
    if data.done
      document.location = data.results
    else
      @onSuccess(data)
      
class InlineLoader
  
  @clickHandler: (event) ->
    event.preventDefault()
    loader = new InlineLoader(this)
    loader.click()
  
  constructor: (el) ->
    @el = el
    @$el = $(el)
    @target = if @$el.data "target" then @$el.closest(@$el.data("target")) else @$el
    
  click: () ->
    $.get @$el.attr("href"), @onResponse
  
  onResponse: (data) =>
    @target.replaceWith data
  
$(() -> new Initialisor())
