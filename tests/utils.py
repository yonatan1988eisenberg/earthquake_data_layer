from dataclasses import dataclass


@dataclass
class MockApiResponse:
    content: dict

    def json(self):
        return self.content
