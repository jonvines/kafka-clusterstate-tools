package net.christophschubert.kafka.clusterstate.formats.cluster;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.Set;

import static net.christophschubert.kafka.clusterstate.formats.Helpers.emptyForNull;

public class Namespace {
    @JsonProperty("name")
    public final String name;

    @JsonProperty("managerPrincipals")
    public final Set<String> managerPrincipals;

    @JsonProperty("consumerPrincipals")
    public final Set<String> consumerPrincipals;

    @JsonProperty("producerPrincipals")
    public final Set<String> producerPrincipals;

    @JsonCreator
    public Namespace(
            @JsonProperty("name") String name,
            @JsonProperty("managerPrincipals") Set<String> managerPrincipals,
            @JsonProperty("consumerPrincipals") Set<String> consumerPrincipals,
            @JsonProperty("producerPrincipals")Set<String> producerPrincipals
    ) {
        this.name = name;
        this.managerPrincipals = emptyForNull(managerPrincipals);
        this.consumerPrincipals = emptyForNull(consumerPrincipals);
        this.producerPrincipals = emptyForNull(producerPrincipals);
    }

    @Override
    public String toString() {
        return "Namespace{" +
                "name='" + name + '\'' +
                ", managerPrincipals=" + managerPrincipals +
                ", consumerPrincipals=" + consumerPrincipals +
                ", producerPrincipals=" + producerPrincipals +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Namespace)) return false;
        Namespace namespace = (Namespace) o;
        return Objects.equals(name, namespace.name) && Objects.equals(managerPrincipals, namespace.managerPrincipals) && Objects.equals(consumerPrincipals, namespace.consumerPrincipals) && Objects.equals(producerPrincipals, namespace.producerPrincipals);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, managerPrincipals, consumerPrincipals, producerPrincipals);
    }
}
