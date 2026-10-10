from gerador.destinos import BUCKET, DestinoLocal, DestinoS3


def test_destino_local_espelha_a_chave_em_pastas(tmp_path):
    destino = DestinoLocal(tmp_path)
    destino.gravar("landing/eventos/dt=2026-09-01/coletor-s42-001.jsonl", '{"a":"é"}\n')
    arquivo = tmp_path / "landing" / "eventos" / "dt=2026-09-01" / "coletor-s42-001.jsonl"
    assert arquivo.read_bytes() == '{"a":"é"}\n'.encode("utf-8")
    assert str(tmp_path.resolve()) in destino.descricao()


class S3Falso:
    def __init__(self):
        self.objetos = {}
        self.buckets = set()

    def head_bucket(self, Bucket):
        if Bucket not in self.buckets:
            raise LookupError(Bucket)

    def create_bucket(self, Bucket, **_):
        self.buckets.add(Bucket)

    def put_object(self, Bucket, Key, Body, ContentType):
        self.objetos[(Bucket, Key)] = (Body, ContentType)


def test_destino_s3_cria_o_bucket_e_grava_utf8():
    s3 = S3Falso()
    destino = DestinoS3(cliente=s3)
    destino.gravar("landing/x.jsonl", "é\n")
    assert BUCKET in s3.buckets
    assert s3.objetos[(BUCKET, "landing/x.jsonl")] == ("é\n".encode("utf-8"), "application/x-ndjson")
    assert destino.descricao().startswith(f"s3://{BUCKET}")
