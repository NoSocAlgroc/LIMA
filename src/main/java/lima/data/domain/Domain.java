package lima.data.domain;

/**
 * Abstract representation of an entire (uninstantiated) space.
 *
 * A Domain doesn't hold any data itself - it's just able to produce
 * instantiated DomainSets (of a chosen size) that live in that space.
 * Concrete implementations fix DS to their own DomainSet implementation, so
 * makeDomainSet returns that concrete type rather than the abstraction.
 */
public interface Domain<DS extends DomainSet<?>> {

    /** Instantiate a DomainSet of `size` elements from this domain. */
    DS makeDomainSet(int size);
}
