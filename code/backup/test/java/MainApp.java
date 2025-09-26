import service.TextProcessingService;

public class MainApp {
    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: java -jar app.jar <input_table> <output_path>");
            System.exit(1);
        }

        String inputTable = args[0];
        String outputPath = args[1];

        TextProcessingService service = null;
        try {
            // 初始化服务
            service = new TextProcessingService();

            // 处理文本数据
            service.processTextData(inputTable, outputPath);

            System.out.println("Processing completed successfully!");
        } catch (Exception e) {
            System.err.println("Error during processing: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (service != null) {
                try {
                    service.close();
                } catch (Exception e) {
                    System.err.println("Error closing resources: " + e.getMessage());
                }
            }
        }
    }
}