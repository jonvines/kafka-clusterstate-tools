package net.christophschubert.kafka.clusterstate.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import net.christophschubert.kafka.clusterstate.ClientBundle;
import net.christophschubert.kafka.clusterstate.ClusterState;
import net.christophschubert.kafka.clusterstate.ClusterStateManager;
import net.christophschubert.kafka.clusterstate.formats.env.CloudCluster;
import net.christophschubert.kafka.clusterstate.formats.env.Environment;
import net.christophschubert.kafka.clusterstate.utils.MapTools;
import org.apache.commons.text.StringSubstitutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;


@Command(name= "kcs", subcommands = { CommandLine.HelpCommand.class }, version = "kcs 0.1.0",
        description = "Manage metadata of a Apache Kafka cluster.")
class CLI {
    private Logger logger = LoggerFactory.getLogger(CLI.class);

    @Command(name = "apply", description = "Apply domain description from context to a cluster")
    int apply(
            @Option(names = { "-b", "--bootstrap-server" }, paramLabel = "<bootstrap-server>",
                    description = "bootstrap server of the cluster to connect too") String bootstrapServer,
            @Option(names = { "-c", "--command-properties"}, paramLabel = "<command properties>",
                    description = "command config file") File configFile,
            @Option(names = { "-e", "--env-var-prefix"}, paramLabel = "<prefix>",
                    description = "prefix for env vars to be added to properties ") String envVarPrefix,
            @CommandLine.Parameters(paramLabel = "context", description = "path to the context", defaultValue = ".") File contextPath
    ) throws IOException, ExecutionException, InterruptedException {
        if (!contextPath.isDirectory()) {
            logger.error("Given context {} is not a folder", contextPath);
            return 1;
        }
        if (configFile == null) {
            configFile = new File(contextPath, "kst.properties");
        }
        final var properties = CLITools.loadProperties(configFile, bootstrapServer, envVarPrefix);

        //perform additional transformations
        final List<Function<ClusterState, ClusterState>> stateTransforms = new ArrayList<>();
        final String principalMappingEnvVarName = "KST_PRINCIPAL_MAPPING";
        if (System.getenv(principalMappingEnvVarName) != null) {
            final var principalMap = CLITools.parsePrincipalMapping(System.getenv(principalMappingEnvVarName));
            stateTransforms.add(cs -> cs.mapPrincipals(principalMap));
        }

        final Runner runner = new Runner(List.of(contextPath), new File(contextPath, "cluster.yaml"), properties, stateTransforms);
        runner.run();

        return 0;
    }

    @CommandLine.Command(name = "applyToSelectedCluster", description = "Apply domain description from context to the selected cluster.")
    int applyToSelectedCluster(
            @CommandLine.Parameters(paramLabel = "environment", description = "path to the environment", defaultValue = ".") File contextPath,
            @CommandLine.Parameters(paramLabel = "clustername", description = "the name of the cluster to work on") String clusterName
    ) {
        final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        try {

            final var environment = mapper.readValue(contextPath, Environment.class);

            environment.clusters.stream().filter( c -> c.name.equals( clusterName ) ).forEach( c -> applyCluster(c, System.getenv()) );

        }
        catch (IOException e) {
            e.printStackTrace();
            return 1;
        }
        return 0;
    }

    @Command(name = "extract", description = "Extract the current state of the cluster")
    void extract(
            @Option(names = { "-b", "--bootstrap-server" }, paramLabel = "<bootstrap-server>",
                    description = "bootstrap server of the cluster to connect too") String bootstrapServer,
            @Option(names = { "-c", "--command-properties"}, paramLabel = "<command properties>",
                    description = "command config file") File configFile,
            @Option(names = { "-e", "--env-var-prefix"}, paramLabel = "<prefix>",
                    description = "prefix for env vars to be added to properties ") String envVarPrefix,
            @Option(names = { "-f", "--file" }, paramLabel = "STATEFILE", description = "filename to store state") File stateFile
    ) throws IOException, ExecutionException, InterruptedException {
        Properties properties = CLITools.loadProperties(configFile, bootstrapServer, envVarPrefix);

        //TODO:
        // remove for production usage: may contain secrets
        logger.info(properties.toString());

        final ClientBundle bundle = ClientBundle.fromProperties(properties);
        final ClusterState clusterState = ClusterStateManager.build(bundle);

        final ObjectMapper mapper = new ObjectMapper();
        mapper.writer().writeValue(stateFile, clusterState);
    }
    /**
     * The following entries from the envVars-map will used for substitutions:
     *  KST_CLUSTER_API_SECRET_<clusterName>
     *  KST_CLUSTER_API_KEY_<clusterName>
     *  KST_SR_API_SECRET_<clusterName>
     *  KST_SR_API_KEY_<clusterName>
     *
     * @param cluster
     * @param envVars
     */
    void applyCluster(CloudCluster cluster, Map<String, String> envVars) {

        final var substitutions = EnvVarTools.extractEnvVarsForCluster(cluster.name, envVars);

        /*
        env vars to create to
         export KST_CLUSTER_API_SECRET_cluster_AO_QA=Q-api-secret
         export KST_CLUSTER_API_KEY_cluster_AO_QA=Q-api-key
         export KST_SR_API_SECRET_cluster_AO_QA=Q-sr-secret
         export KST_SR_API_KEY_cluster_AO_QA=Q-sr-key
         */

        final StringSubstitutor substitutor = new StringSubstitutor(substitutions);

        final var clientProps = new Properties();
        clientProps.putAll(MapTools.mapValues(CLITools.getClientProps(cluster), substitutor::replace));

        //build principal mapping
        final List<Function<ClusterState, ClusterState>> stateTransforms = Collections.singletonList(cs -> cs.mapPrincipals(cluster.principals));

        try {

            File clusterWideACLs = null;

            if( cluster.clusterLevelAccessPath != null ) {
                clusterWideACLs = new File(cluster.clusterLevelAccessPath);
            }

            //todo: fix this
            final var folders = cluster.domainFileFolders.stream().map(File::new).collect(Collectors.toList());
            final Runner runner = new Runner(folders, clusterWideACLs , clientProps, stateTransforms);
            runner.run();
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (ExecutionException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static void main(String[] args) {
        final int status = new CommandLine(new CLI()).execute(args);
        System.exit(status);
    }
}
