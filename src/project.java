import javax.swing.*;
import java.awt.*;
import java.awt.event.*;
import java.util.*;
import java.io.*;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.gax.paging.Page;
import com.google.api.services.dataproc.*;
import com.google.api.services.dataproc.model.HadoopJob;
import com.google.api.services.dataproc.model.Job;
import com.google.api.services.dataproc.model.JobPlacement;
import com.google.api.services.dataproc.model.SubmitJobRequest;
import com.google.cloud.storage.Storage.*;
import com.google.api.services.storage.model.StorageObject;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.cloud.storage.*;
import com.google.cloud.storage.Blob;
import java.io.FileInputStream;
import java.nio.file.Paths;

class App {
  JFrame frame=new JFrame("IPH3 Search Engine");
  JPanel panel = new JPanel();
  JPanel p2 = new JPanel();
  JPanel p3 = new JPanel();
  JTextArea ta = new JTextArea();

  JButton button;
  JButton searchbtn;
  JButton topbtn;
  JLabel label;
  JLabel label2;

  App() {
    start();
  }

  public void start() {

    frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
    frame.setSize(600,400);

    ButtonGroup bg = new ButtonGroup();
    JRadioButton hugo = new JRadioButton("Hugo");
    JRadioButton shake = new JRadioButton("Shakespeare");
    JRadioButton tols = new JRadioButton("Tolstoy");
    JRadioButton all = new JRadioButton("All");
    all.setSelected(true);
    bg.add(hugo);
    bg.add(shake);
    bg.add(tols);
    bg.add(all);

    // entry page
    label = new JLabel("Load My Engine");
    button = new JButton("Start");
    panel.add(label);
    p2.add(button);
    button.addActionListener(new ActionListener() {
      public void actionPerformed(ActionEvent e) {

        button.setVisible(false);
        label.setText("Engine was loaded & Inverted Indicies were constructed successfully!");
        label2 = new JLabel("Please select an action:");
        p2.add(label2);

        searchbtn = new JButton("Search for Term");
        topbtn = new JButton("Top-N");

        // search for term
        p3.add(searchbtn);
        searchbtn.addActionListener(new ActionListener() {
          public void actionPerformed(ActionEvent e) {
            label.setText("Choose which Author's text to search:");
            searchbtn.setVisible(false);
            topbtn.setVisible(false);
            label2.setText("Enter Your Search Term:");
            p2.add(hugo);
            p2.add(shake);
            p2.add(tols);
            p2.add(all);
            p3.add(label2);
            JButton s = new JButton("Search");
            JTextField tf = new JTextField(16);
            p3.add(tf);
            p3.add(s);
            // when search button is clicked
            s.addActionListener(new ActionListener() {
              public void actionPerformed(ActionEvent e) {
            	  String term = tf.getText();
            	  label.setText("You searched for the term: " + term);
            	  label2.setText("Your search was executed in ms");
            	  p2.add(label2);
            	  s.setVisible(false);
            	  tf.setVisible(false);
            	  hugo.setVisible(false);
            	  shake.setVisible(false);
            	  tols.setVisible(false);
            	  all.setVisible(false);
              }
            });
          }
        });

        // top-n
        p3.add(topbtn);
        topbtn.addActionListener(new ActionListener() {
          public void actionPerformed(ActionEvent e) {
            label.setText("Choose which Author's text to search:");
            searchbtn.setVisible(false);
            topbtn.setVisible(false);
            label2.setText("Enter Your N Value:");
            p2.add(hugo);
            p2.add(shake);
            p2.add(tols);
            p2.add(all);
            p3.add(label2);
            JButton s2 = new JButton("Search");
            JTextField tf2 = new JTextField(16);
            p3.add(tf2);
            p3.add(s2);
            s2.addActionListener(new ActionListener() {
                public void actionPerformed(ActionEvent e) {
              	  String ns = tf2.getText();
              	  int n = Integer.parseInt(ns);
              	  label.setText("Top-N Frequent Terms");
              	  label2.setVisible(false);
              	  s2.setVisible(false);
              	  tf2.setVisible(false);
              	  hugo.setVisible(false);
              	  shake.setVisible(false);
              	  tols.setVisible(false);
              	  all.setVisible(false);
              	  
              	  // get source to be used for the input
              	  String inSource = "";
              	  if(hugo.isSelected()) {
              		  inSource = "Hugo/";
              	  } else if(shake.isSelected()) {
              		  inSource = "Shakespeare/";
              	  } else if(tols.isSelected()) {
              		  inSource = "Tolstoy/";
              	  }
              	  // if all button is selected then string stays empty as intended
              	  
              	  String inputurl = "gs://dataproc-staging-us-central1-688118264243-sjzrg4jj/Data/" + inSource;
              	  
	              	try {
	              		// connect to gcp
	                    GoogleCredentials credentials = GoogleCredentials.fromStream(new FileInputStream("/Users/bellahilty/eclipse-workspace/iph3_course_project/src/credentials.json"))
	                            .createScoped(Lists.newArrayList("https://www.googleapis.com/auth/cloud-platform"));
	                    HttpRequestInitializer requestInitializer = new HttpCredentialsAdapter(credentials);
	                    Dataproc dataproc = new Dataproc.Builder(new NetHttpTransport(),new JacksonFactory(), requestInitializer).build();
	                    // create new job for cluster
	                    HadoopJob h = new HadoopJob();
	                    h.setMainJarFileUri("gs://dataproc-staging-us-central1-688118264243-sjzrg4jj/JAR/processor2.jar");
	                    // input url goes here to get folder path for whichever data is being used
	                    h.setArgs(ImmutableList.of("processor",
	                                 inputurl,
	                                 "gs://dataproc-staging-us-central1-688118264243-sjzrg4jj/output"));
	                    
	                    dataproc.projects().regions().jobs().submit("cs1660-296421" , "us-central1", new SubmitJobRequest()
	                                 .setJob(new Job()
	                                    .setPlacement(new JobPlacement()
	                                        .setClusterName("cluster-1"))
	                                 .setHadoopJob(h)))
	                                .execute();
	                    System.out.println("job finished successfully");
	                } catch (Exception ex) {
	                    System.out.println("Error!" + ex);
	                    ex.printStackTrace();
	                }
	              	
	              	try {
	              		GoogleCredentials credentials = GoogleCredentials.fromStream(new FileInputStream("/Users/bellahilty/eclipse-workspace/iph3_course_project/src/credentials.json"))
	                            .createScoped(Lists.newArrayList("https://www.googleapis.com/auth/cloud-platform"));
	              		Storage storage = StorageOptions.newBuilder().setCredentials(credentials).build().getService();
	              		
	              		StorageObject out = new StorageObject();
	                    out.setBucket("dataproc-staging-us-central1-688118264243-sjzrg4jj");
	                    out.setName("output.txt");
	                    Storage.ComposeRequest.Builder requestBuilder = new Storage.ComposeRequest.Builder();
	                    
	                    Page<Blob> b = storage.list("dataproc-staging-us-central1-688118264243-sjzrg4jj", BlobListOption.currentDirectory(), BlobListOption.prefix("output/"));
			            Iterator<Blob> iterator = b.iterateAll().iterator();
			            ArrayList<String> blobList = new ArrayList<>();
			            ArrayList<BlobId> blobIDs = new ArrayList<>();
			            while(iterator.hasNext()) {
			                Blob currBlob = iterator.next();
			                blobIDs.add(currBlob.getBlobId());
			                String name = currBlob.getName();
			                // merge all output files together except for success file
			                if (!name.equals("output/_SUCCESS")) {
			                    //System.out.println("adding " + name);
			                    blobList.add(name);
			                }
			            }
			            
			            requestBuilder.addSource(blobList);
			            
			            requestBuilder.setTarget(
			                    BlobInfo.newBuilder("dataproc-staging-us-central1-688118264243-sjzrg4jj", "output.txt").build());
			            Storage.ComposeRequest request = requestBuilder.build();
			            
			            BlobInfo comp = storage.compose(request);
			           
			            storage.delete(blobIDs);	// delete out file if one exists
			            
			            Blob blob = storage.get(BlobId.of("dataproc-staging-us-central1-688118264243-sjzrg4jj", "output.txt"));
			            blob.downloadTo(Paths.get("./output.txt"));
			            
			            Scanner in = new Scanner(new File("output.txt"));
			            p3.add(ta);
			            int count = 0;
			            // output top n terms
			            while(in.hasNextLine() && count < n) {
			                count++;
			                String curr = in.nextLine();
			                ta.append(curr + "\n");
			            }
			            ta.repaint();
	              	} catch (Exception ex) {
	              		System.out.println("ERROR: " + ex);
	              		ex.printStackTrace();
	              	}
              	
                 }
              });
          }
        });
      }
    });

    frame.getContentPane().add(BorderLayout.NORTH, panel);
    panel.setBorder(BorderFactory.createEmptyBorder(50,100,50,50));
    frame.getContentPane().add(BorderLayout.CENTER, p2);
    frame.getContentPane().add(BorderLayout.SOUTH, p3);
    p3.setBorder(BorderFactory.createEmptyBorder(0,50,50,50));
    frame.setVisible(true);


  }

}

////////////////////////////////////////////////

public class project {
  public static void main(String[] args) {
    new App();
  }
}
