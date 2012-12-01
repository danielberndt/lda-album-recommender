(function() {
  var Connector, Form, Initialisor, InlineLoader,
    __bind = function(fn, me){ return function(){ return fn.apply(me, arguments); }; };

  Connector = (function() {

    function Connector(address) {
      this.onreceive = __bind(this.onreceive, this);
      var WS;
      WS = window['MozWebSocket'] ? MozWebSocket : WebSocket;
      this.socket = new WS(address);
      this.socket.onmessage = this.onreceive;
      this.$msg = $(".msg").show();
    }

    Connector.prototype.onreceive = function(event) {
      var data, typeString;
      data = JSON.parse(event.data);
      if (data.done) {
        return window.location = data.results;
      } else {
        if (data.message.type === "error") {
          $("<li></li>").addClass("error-message").text("an error has occurred: " + data.message.extra).appendTo(this.$msg);
          return this.$msg.addClass("error");
        } else if (data.message.done) {
          return $(".msg-" + data.message.type + "-" + data.message.page, this.$msg).addClass("done");
        } else {
          typeString = data.message.type === "all" ? "all your music" : "your current music";
          return $("<li></li>").addClass("msg-" + data.message.type + "-" + data.message.page).text("crawling " + typeString + " on page " + data.message.page).appendTo(this.$msg);
        }
      }
    };

    return Connector;

  })();

  window["Connector"] = Connector;

  Initialisor = (function() {

    function Initialisor() {
      $(document).on("submit", "form", Form.submissionHandler);
      $(document).on("click", "a.js-inline", InlineLoader.clickHandler);
    }

    return Initialisor;

  })();

  Form = (function() {

    Form.submissionHandler = function(event) {
      var form;
      event.preventDefault();
      form = new Form(this);
      return form.submit();
    };

    function Form(el) {
      this.onUpdateResponse = __bind(this.onUpdateResponse, this);
      this.onError = __bind(this.onError, this);
      this.onSuccess = __bind(this.onSuccess, this);      this.el = el;
      this.$el = $(el);
    }

    Form.prototype.submit = function() {
      $(".error", this.$el).remove();
      $.ajax({
        type: this.$el.attr("method"),
        url: this.$el.attr("action"),
        data: this.$el.serialize(),
        dataType: "json",
        success: this.onSuccess,
        error: this.onError
      });
      return this.$el.addClass("pending");
    };

    Form.prototype.onSuccess = function(data) {
      this.$el.removeClass("pending");
      if (window['WebSocket'] || window['MozWebSocket']) {
        return new Connector(data.ws_update);
      } else {
        $(".msg").show().html("<li>Loading...</li><li><small>PS: you really should get a modern browser that supports web sockets!</small></li>");
        return $.ajax({
          type: "get",
          url: String(data.update),
          dataType: "json",
          success: this.onUpdateResponse
        });
      }
    };

    Form.prototype.onError = function(xhr) {
      this.$el.removeClass("pending");
      return $("<div></div>").text(JSON.parse(xhr.response).error).addClass("error").appendTo(this.$el);
    };

    Form.prototype.onUpdateResponse = function(data) {
      if (data.done) {
        return document.location = data.results;
      } else {
        return this.onSuccess(data);
      }
    };

    return Form;

  })();

  InlineLoader = (function() {

    InlineLoader.clickHandler = function(event) {
      var loader;
      event.preventDefault();
      loader = new InlineLoader(this);
      return loader.click();
    };

    function InlineLoader(el) {
      this.onResponse = __bind(this.onResponse, this);      this.el = el;
      this.$el = $(el);
      this.target = this.$el.data("target") ? this.$el.closest(this.$el.data("target")) : this.$el;
    }

    InlineLoader.prototype.click = function() {
      return $.get(this.$el.attr("href"), this.onResponse);
    };

    InlineLoader.prototype.onResponse = function(data) {
      return this.target.replaceWith(data);
    };

    return InlineLoader;

  })();

  $(function() {
    return new Initialisor();
  });

}).call(this);
