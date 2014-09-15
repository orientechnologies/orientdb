package com.orientechnologies.orient.unit;

public class ConnectionInfo {
    private String address = "localhost";
    private int binaryPorts[] = {2424};
    private String login = "admin";
    private String password = "admin";

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public int[] getBinaryPorts() {
        return binaryPorts;
    }

    public void setBinaryPorts(int[] binaryPorts) {
        this.binaryPorts = binaryPorts;
    }

    public String getLogin() {
        return login;
    }

    public void setLogin(String login) {
        this.login = login;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    protected static void throwIllegalStateIf(boolean condition, String template, Object...args){
        if(condition){
            throw new IllegalStateException(String.format(template, args));
        }
    }

    public void verify() throws IllegalStateException{
        throwIllegalStateIf(address == null || address.isEmpty(), "Address must be specified");
        throwIllegalStateIf(binaryPorts == null || binaryPorts.length < 1, "At least one binary port must be specified");
        throwIllegalStateIf(login == null || login.isEmpty(), "User login must be specified");
        throwIllegalStateIf(password == null || password.isEmpty(), "User's password must be specified");
    }

    public static class ConnectionInfoBuilder<T extends ConnectionInfoBuilder, I extends ConnectionInfo> {
        protected final I buildInfo;

        public ConnectionInfoBuilder(ConnectionInfo buildInfo) {
            this.buildInfo = (I)buildInfo;
        }

        public T useCredential(String login, String password){
            buildInfo.setLogin(login);
            buildInfo.setPassword(password);
            return (T)this;
        }

        public T useAddress(String address){
            buildInfo.setAddress(address);
            return (T) this;
        }

        public T useBinaryPort(int port){
            buildInfo.setBinaryPorts(new int[]{port});
            return (T)this;
        }
    }

}
