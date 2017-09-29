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

});
