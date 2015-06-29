package pl.worker;


import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.simpledb.AmazonSimpleDBClient;
import com.amazonaws.services.simpledb.model.PutAttributesRequest;
import com.amazonaws.services.simpledb.model.ReplaceableAttribute;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.imageio.ImageIO;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author Matt
 */
public class Main {

    public static void main(String[] args) {
        while (true) {
            final String file = getMessage("https://sqs.us-west-2.amazonaws.com/983680736795/leszczynskaSQS");
            if (file == null) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ex) {
                    Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
                }
                continue;
            }
            Runnable thread = new Runnable() {

                @Override
                public void run() {
                    try {
                        BufferedImage image =  rotate90ToRight(getFile(file));
                        putFile(image, file);
                        putToSDB(file);
                    } catch (IOException ex) {
                        Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            };
            
            Thread t = new Thread(thread);
            t.start();
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
                Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    public static String getMessage(String url) {
        final AmazonSQS sqsClient = new AmazonSQSClient();
        final ReceiveMessageRequest request = new ReceiveMessageRequest(url);
        request.setMaxNumberOfMessages(1);
        ReceiveMessageResult result = sqsClient.receiveMessage(request);
        List<Message> messages = result.getMessages();
        if (messages.isEmpty()) {
            return null;
        }
        String body = messages.get(0).getBody();
        sqsClient.deleteMessage(url, messages.get(0).getReceiptHandle());
        return body;
    }
   

    public static BufferedImage getFile(String fileName) throws IOException {
        AmazonS3 s3Client = new AmazonS3Client();
        S3Object s3Object = s3Client.getObject("lab4-weeia", "agnieszka.leszczynska/"+fileName);
        return ImageIO.read(s3Object.getObjectContent());
    }

    public static BufferedImage rotate90ToRight(BufferedImage inputImage) {
        int width = inputImage.getWidth();
        int height = inputImage.getHeight();
        BufferedImage returnImage = new BufferedImage(height, width, inputImage.getType());

        for (int x = 0; x < width; x++) {
            for (int y = 0; y < height; y++) {
                returnImage.setRGB(height - y - 1, x, inputImage.getRGB(x, y));
            }
        }
        return returnImage;
    }
    
    public static String generateNewNameOfFile(String file)
    {
        return "rotated_"+file;
    }
    
    public static void putFile(BufferedImage image, String file) throws IOException
    {
        AmazonS3 s3Client = new AmazonS3Client();
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        ImageIO.write(image, "gif", os);
        InputStream is = new ByteArrayInputStream(os.toByteArray());
        s3Client.putObject("lab4-weeia", "agnieszka.leszczynska/"+generateNewNameOfFile(file), is, null);
    }
    
    public static void putToSDB(String file)
    {
        AmazonSimpleDBClient simpleDB = new AmazonSimpleDBClient();
        simpleDB.setRegion(Region.getRegion(Regions.US_WEST_2));
        simpleDB.putAttributes(new PutAttributesRequest("leszczynska_project", "Przetworzono plik", Arrays.asList(new ReplaceableAttribute("key", file, Boolean.FALSE))));
    }
}
