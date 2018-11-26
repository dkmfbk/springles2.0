package eu.fbk.dkm.springles;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryException;

// SParql Rule-based Inference over Named Graphs Layer Extending Sesame

// three sets of properties:
// - read-only, directly or indirectly set at construction time, always readable
// - configurable properties (the ones for which a setter is provided), settable before
// initialization and always readable
// - derived properties, whose value is derived/generated after initialization, thus readable only
// after initialization (isWritable, getInferenceMode, getValueFactory)

// another difference w.r.t. sesame: in update operations, the default remove graph is not the
// union of all Sesame contexts, but just the null context (otherwise, DELETE DATA and MODIFY
// update operation would need to be rewritten multiplying statement patterns in delete clauses
// for each explicit context in the repository).
/**
 * Extension of <tt>Repository</tt> with additional inference control.
 * 
 * @apiviz.landmark
 * @apiviz.uses eu.fbk.dkm.springles.SPC - - <<auxiliary>>
 * @apiviz.uses eu.fbk.dkm.springles.SpringlesConnection - - <<creates>>
 */
public interface SpringlesRepository extends Repository
{

    QueryLanguage PROTOCOL = QueryLanguage.register("SPRINGLES");

    String TYPE = "springles:Repository";

    String getID();

    IRI getNullContextURI();

    String getInferredContextPrefix();

    // Accessible only after initialization

    @Override
    boolean isWritable();

    InferenceMode getInferenceMode();

    @Override
    public ValueFactory getValueFactory();

    @Override
    SpringlesConnection getConnection() throws RepositoryException;

}
