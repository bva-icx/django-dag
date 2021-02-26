
class BaseDagOrderController():
    """
    Interface class to provide support for edge or node ordering.
    """
    def __init__(self, sequence_field_name='sequence'):
        self.sequence_field_name = sequence_field_name

    ####################################################################
    # Functions needed for construction
    def get_node_sequence_field(self, ):
        """
        Returns a single field to be used to support ordering.
        Should the controller require more fields this can be a primary key
        to another model
        """
        raise NotImplementedError

    def get_edge_sequence_field(self, ):
        """
        Returns a single field to be used to support ordering.
        Should the controller require more fields this can be a primary key
        to another model
        """
        raise NotImplementedError


    ####################################################################
    # Queries related to a node
    def get_relatedsort_query_component(self, model, target, source):
        """
        Builds a query component that can be used for sorting a children of
        a dag Node.

        If the model is a instance of a DAG node the children should be
        so sorted in relation to the instance, if the model is a class then
        all result we be the indeterminate although in the reference implementation
        of edgeordering it is the first sequence. The precise details are
        implementation specific to the the concrete sequence_manager 

        :param model: A Node model or model instance
        :return: A componet to be used in a further query 
            F, Subquery etc. which will result in a value for the node sequence
        """
        raise NotImplementedError
