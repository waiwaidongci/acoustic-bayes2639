from ..services import federation
from ..utils.serialization import row
class FederationController:
    list_rounds=staticmethod(federation.list_rounds)
    create_round=staticmethod(federation.create_round)
    round_detail=staticmethod(federation.round_detail)
    submit=staticmethod(federation.submit_contribution)
    seal=staticmethod(federation.seal_round)
    publish=staticmethod(federation.publish_round)
    preview=staticmethod(federation.preview_stats)
    interpret=staticmethod(federation.interpret_with_published)
    @staticmethod
    def model(db):return row(federation.current_model(db))
