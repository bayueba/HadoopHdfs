package rpc;

import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

public class LoginController {

    public static void main(String[] args) throws Exception {
        LoginServiceInterface proxy = RPC.getProxy(LoginServiceInterface.class, 1L, new InetSocketAddress("192.168.37.101", 9000), new Configuration());

        String result = proxy.login("root", "123");

        System.out.println(result);
    }


}