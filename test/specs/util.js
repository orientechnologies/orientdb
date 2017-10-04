exports.logout = function (browser) {
  browser.waitForVisible("#user-dropdown", true);
  browser.click("#user-dropdown")
    .waitForVisible("#logout-button", true);

  browser.click("#logout-button")
    .waitForVisible(".ologin");

  return browser;
}

exports.enter = function (browser) {
  browser.url("/")
    .waitForExist(".ologin", true);

  return browser;
}


exports.createDatabase = function (browser, name) {
  browser
    .click("#new-database-button")
    .waitForExist("#new-db-name", true);

  browser
    .setValue('#new-db-name', name)
    .setValue('#serverUser', "root")
    .setValue('#serverPassword', "root")
    .click('#new-database-create-button')
    .waitForExist(".browse-container", true);

  return browser;
}


exports.dropDatabase = function (browser, name, wait) {


  browser
    .selectByValue('#database-selection', "string:" + name)
    .click('#database-drop-button')
    .waitForExist("#delete-db-form", true);

  browser
    .setValue('#serverUser', "root")
    .setValue('#serverPassword', "root")
    .click("#database-delete-confirm-button")
    .waitForValue("#database-selection", wait);

  return browser;
}
