import backtype.storm.utils.DRPCClient;

public class DemoClient {
    public static void main(String[] args) throws Exception {
        DRPCClient client = new DRPCClient("localhost", 3772);
        String[] words = {"hello", "storm", "drpc"};
        for (String word : words) {
            String result = client.execute("j-exclamation", word);
            System.out.println("Result for \"" + word + "\": " + result);
        }

//        String[] urlsToTry = new String[]{"foo.com/blog/1", "engineering.twitter.com/blog/5", "notaurl.com"};
//        for (String url : urlsToTry) {
//            String result = client.execute("reach", url);
//            System.out.println("Reach of " + url + ": " + result);
//        }
    }
}