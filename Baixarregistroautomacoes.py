import sys
import os
import time
import shutil
import tempfile
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, List

import pytz
import pandas as pd
from pandas.api.types import is_datetime64_any_dtype, is_numeric_dtype
from google.cloud import bigquery
import pythoncom
from win32com.client import Dispatch

# se mudar pra DEBUG vai explodir de linhas no log, INFO é mais de boa
LOG_LEVEL = logging.INFO

logger = logging.getLogger(Path(__file__).stem)
logger.propagate = False

# se trocar destinatário aqui vira outro e-mail, se for lista, vira string separada por ; lá na função
DESTINATARIOS_OUTLOOK = "carlos.lsilva@c6bank.com"

# se alterar o projeto/tabela vai puxar de outro lugar, óbvio, então cuidado pra não trocar sem querer
PROJETO_BQ = "datalab-pagamentos"
TABELA_BQ = "datalab-pagamentos.ADMINISTRACAO_CELULA_PYTHON.Registro_automacoes"

# se colocar False aqui ele não apaga a pasta de destino, só sobrescreve o arquivo se tiver mesmo nome (pode sobrar sujeira)
APAGAR_PASTA_REGISTRO = True

# se trocar timezone, muda saudação/horário mostrado no e-mail, aí fica estranho pro time do BR
TZ_BR = pytz.timezone("America/Sao_Paulo")

TEMP_DIR: Optional[Path] = None
LOG_PATH: Optional[Path] = None


def _setup_logger() -> None:
    # se mudar prefixo aqui o nome da pasta temp muda, o que é ok, mas o .log sempre vai pra essa pasta
    global TEMP_DIR, LOG_PATH
    TEMP_DIR = Path(tempfile.mkdtemp(prefix=f"{Path(__file__).stem.upper()}_"))
    ts = datetime.now(TZ_BR).strftime("%Y%m%d_%H%M%S")
    LOG_PATH = TEMP_DIR / f"{Path(__file__).stem.upper()}_{ts}.log"

    logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
    for h in list(logger.handlers):
        logger.removeHandler(h)
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    fh = logging.FileHandler(LOG_PATH, encoding="utf-8")
    fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(ch)
    logger.addHandler(fh)
    logger.setLevel(LOG_LEVEL)

    logger.info("inicio do script")
    logger.info("temp: %s", TEMP_DIR)


def _saudacao() -> str:
    # se quiser padronizar outra saudação é aqui, mas o padrão do time é usar isso
    hora = datetime.now(TZ_BR).hour
    if hora < 12:
        return "Olá, bom dia"
    if hora < 18:
        return "Olá, boa tarde"
    return "Olá, boa noite"


def garantir_outlook_aberto() -> bool:
    # se Outlook não estiver configurado, isso aqui volta False e o script segue sem mandar e-mail
    try:
        pythoncom.CoInitialize()
    except Exception:
        pass
    try:
        Dispatch("Outlook.Application")
        return True
    except Exception:
        logger.warning("nao deu pra iniciar o outlook")
        return False


def procurar_arquivos() -> List[Path]:
    # aqui não tem procura real no script 1, retorna lista vazia só pra manter a interface padrão exigida
    return []


def regravar_excel(path_xlsx: Path) -> Path:
    # se mudar pra fazer cópia/otimização, cuidado pra não mudar valores; hoje só devolve o próprio caminho
    return path_xlsx


def tratar_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    - Mantém colunas numéricas e de data sem alteração.
    - Demais colunas viram TEXTO em MAIÚSCULO (exceto valores nulos).
    - Cabeçalhos não são alterados.
    """
    df = df.copy()

    for col in df.columns:
        serie = df[col]

        # pula colunas numéricas e de data
        if is_numeric_dtype(serie) or is_datetime64_any_dtype(serie):
            continue

        # garante dtype objeto pra poder guardar texto
        serie_convertida = serie.astype("object")

        # somente valores não nulos
        mask_validos = serie_convertida.notna()
        serie_convertida.loc[mask_validos] = (
            serie_convertida.loc[mask_validos]
            .astype(str)
            .str.upper()
        )

        df[col] = serie_convertida

    logger.info(
        "tratamento dataframe concluido: %d colunas preservadas (numericas/datas), %d colunas texto upper",
        sum(is_numeric_dtype(df[c]) or is_datetime64_any_dtype(df[c]) for c in df.columns),
        sum(not (is_numeric_dtype(df[c]) or is_datetime64_any_dtype(df[c])) for c in df.columns),
    )

    return df


def subir_bq(_: pd.DataFrame) -> int:
    # no script 1 não subimos nada, essa função é placeholder pra cumprir a ordem pedida
    return 0


def rodar_procedures() -> None:
    # se um dia precisar, coloca a call aqui, mas hoje é no-op
    return


def _assunto_email() -> str:
    # se mudar formatação aqui, o título foge do padrão cobrado na monitoracão
    agora = datetime.now(TZ_BR)
    data = agora.strftime("%d//%m//%Y")
    hora = agora.strftime("%H:%M:%S")
    nome = Path(__file__).stem.upper()
    return f"Célula Python - Monitoracao - {nome} - {data} - {hora}"


def _corpo_email(rows: int, inicio_exec: datetime, fim_exec: datetime) -> str:
    # se remover linhas daqui, o corpo sai do padrão obrigatório (o pessoal reclama)
    saud = _saudacao()
    data_exec = inicio_exec.astimezone(TZ_BR).strftime("%d/%m/%Y %H:%M:%S")
    dur = (fim_exec - inicio_exec).total_seconds()
    hh = int(dur // 3600)
    mm = int((dur % 3600) // 60)
    ss = int(dur % 60)
    tempo = f"{hh:02d}:{mm:02d}:{ss:02d}"
    return (
        f"<html><body style='font-family:Arial,sans-serif;'>"
        f"<p>{saud}</p>"
        f"<p>Linhas processadas: {rows}</p>"
        f"<p>Tabela referência: {TABELA_BQ}</p>"
        f"<p>Tempo execução: {tempo}</p>"
        f"</body></html>"
    )


def _anexos(extra: Optional[List[Path]] = None) -> List[Path]:
    # se trocar a regra do anexo, por favor mantenha o .log obrigatorio
    anexos: List[Path] = []
    if LOG_PATH and LOG_PATH.exists():
        anexos.append(LOG_PATH)
    if extra:
        for p in extra:
            try:
                if p and p.exists():
                    anexos.append(p)
            except Exception:
                logger.warning("falha ao preparar anexo: %s", p)
    return anexos


def enviar_email(rows: int, inicio_exec: datetime, fim_exec: datetime, extra_anexos: Optional[List[Path]] = None) -> None:
    # se mudar para False aqui, mesmo com Outlook ok, não envia nada (não recomendado pra monitoração)
    if not garantir_outlook_aberto():
        logger.warning("outlook indisponivel, e-mail nao enviado")
        return
    try:
        outlook = Dispatch("Outlook.Application")
        mail = outlook.CreateItem(0)
        mail.To = DESTINATARIOS_OUTLOOK if isinstance(DESTINATARIOS_OUTLOOK, str) else ";".join(DESTINATARIOS_OUTLOOK)
        mail.Subject = _assunto_email()
        mail.HTMLBody = _corpo_email(rows, inicio_exec, fim_exec)
        for a in _anexos(extra_anexos):
            try:
                mail.Attachments.Add(str(a))
            except Exception:
                logger.warning("nao consegui anexar: %s", a)
        mail.Send()
        logger.info("email enviado: %s", mail.Subject)
    except Exception:
        logger.exception("falha ao enviar email")


def _pasta_registro_downloads() -> Path:
    # se mudar pra outra pasta, o segundo script nao vai achar o arquivo pra subir depois
    downloads = Path.home() / "Downloads"
    return downloads / "registro_automacoes"


def _limpar_e_criar_destino(dest: Path) -> None:
    # se colocar APAGAR_PASTA_REGISTRO=False ele mantem arquivos antigos, o que pode confundir validacao
    if APAGAR_PASTA_REGISTRO and dest.exists():
        shutil.rmtree(dest, ignore_errors=True)
    dest.mkdir(parents=True, exist_ok=True)


def main() -> int:
    _setup_logger()
    inicio_exec = datetime.now(timezone.utc)
    ret = 0
    xlsx_path: Optional[Path] = None

    try:
        logger.info("consultando bigquery: %s", TABELA_BQ)
        client = bigquery.Client(project=PROJETO_BQ)
        query = f"SELECT * FROM `{TABELA_BQ}`"
        df = client.query(query).result().to_dataframe(create_bqstorage_client=False)  # sem mexer em dtype

        # TRATAMENTO: texto em maiúsculo, numéricos/datas preservados
        df = tratar_dataframe(df)

        destino = _pasta_registro_downloads()
        _limpar_e_criar_destino(destino)

        ts = datetime.now(TZ_BR).strftime("%Y%m%d_%H%M%S")
        xlsx_path = destino / f"registro_automacoes_{ts}.xlsx"

        logger.info("gravando xlsx: %s", xlsx_path)
        with pd.ExcelWriter(xlsx_path, engine="xlsxwriter") as writer:
            df.to_excel(writer, index=False, sheet_name="dados")

        enviar_email(
            rows=len(df),
            inicio_exec=inicio_exec,
            fim_exec=datetime.now(timezone.utc),
            extra_anexos=[xlsx_path],
        )
        ret = 0
        return ret

    except Exception:
        logger.exception("falha geral na exportacao")
        try:
            enviar_email(
                rows=0,
                inicio_exec=inicio_exec,
                fim_exec=datetime.now(timezone.utc),
                extra_anexos=[p for p in [xlsx_path] if p],
            )
        except Exception:
            pass
        ret = 1
        return ret

    finally:
        try:
            if TEMP_DIR and TEMP_DIR.exists():
                shutil.rmtree(TEMP_DIR, ignore_errors=True)
        except Exception:
            pass


if __name__ == "__main__":
    sys.exit(main())
