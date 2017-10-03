'use strict';
var config = require('./config');

describe("HomePage", function () {


  before(function () {

  });

  // --------------------------------------------------------------------------
  // Mocha
  // --------------------------------------------------------------------------
  // Set a Mocha global timeout of 10 seconds to allow for slow tests/tunnels
  this.timeout(250000);

  it("It should have a correct title", function (done) {
    browser.url("/");
    var title = browser.getTitle();
    expect(title).to.equal("OrientDB Studio");
  });


  it("It should login to the default database", function (done) {
    browser.url("/")
      .waitForExist(".ologin", true);

    browser.setValue('#user', "admin")
      .setValue('#password', "admin")
      .click("#database-connect")
      .waitForExist(".browse-container", true);
  });

  it("It should login to the default database and then logout", function (done) {
    browser.url("/")
      .waitForExist(".ologin", true);

    browser.setValue('#user', "admin")
      .setValue('#password', "admin")
      .click("#database-connect")
      .waitForExist(".browse-container", true);

    browser.waitForVisible("#user-dropdown", true);
    browser.click("#user-dropdown")
      .waitForVisible("#logout-button", true);

    browser.click("#logout-button")
      .waitForVisible(".ologin");
  });

  it("It should login with reader/reader to the default database and op fail", function (done) {
    browser.url("/")
      .waitForExist(".ologin", true);

    browser.setValue('#user', "reader")
      .setValue('#password', "reader")
      .click("#database-connect")
      .waitForExist(".browse-container", true);


    browser.execute(function () {
      var codemirror = document.querySelector('.CodeMirror').CodeMirror;
      codemirror.setValue("insert into v set name = 'Test'");
    });

    browser.click("#button-run")
      .waitForVisible('.noty_text');

    var inputUser = browser.getHTML('.noty_text', false);

    var message = 'The command has not been executed';
    // var message = `com.orientechnologies.orient.core.exception.OSecurityAccessException: User 'reader' does not have permission to execute the operation 'Create' against the resource: ResourceGeneric [name=CLASS, legacyName=database.class].V DB name="${config.defaultDB}"`

    expect(inputUser).to.equal(message);

  });

});
