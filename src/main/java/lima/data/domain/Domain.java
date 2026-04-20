package lima.data.domain;

/**
 * An uninstantiated space that can produce DomainSets of a requested size.
 * Implementations bind DS to their concrete DomainSet type.
 */
public interface Domain<DS extends DomainSet<?>> {

    /** Instantiate a DomainSet of `size` elements from this domain. */
    DS makeDomainSet(int size);
}
