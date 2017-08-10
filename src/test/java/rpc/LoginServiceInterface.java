package rpc;

/**
 * Created by yqy on 17-8-2.
 */
public interface LoginServiceInterface {
    public static final long versionID=1L;
    public String login(String username,String password);
}


