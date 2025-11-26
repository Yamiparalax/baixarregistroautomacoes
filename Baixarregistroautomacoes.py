import getpass
import logging
import os
import shutil
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional, Tuple
from zoneinfo import ZoneInfo

import pandas as pd
from google.cloud import bigquery


NOME_AUTOMACAO = "BAIXAR_REGISTRO_AUTOMACOES"
NOME_SCRIPT = Path(__file__).stem.upper()
NOME_SERVIDOR = "Servidor.py"
TZ = ZoneInfo("America/Sao_Paulo")
INICIO_EXEC_SP = datetime.now(TZ)
DATA_EXEC = INICIO_EXEC_SP.date().isoformat()
HORA_EXEC = INICIO_EXEC_SP.strftime("%H:%M:%S")
NAVEGADOR_ESCONDIDO = False
REGRAVAREXCEL = False
MODO_SUBIDA_BQ = "append"
RETCODE_SUCESSO = 0
RETCODE_FALHA = 1
RETCODE_SEMDADOSPARAPROCESSAR = 2
BQ_TABELA_DESTINO = "datalab-pagamentos.ADMINISTRACAO_CELULA_PYTHON.Registro_automacoes"
BQ_TABELA_METRICAS = "datalab-pagamentos.ADMINISTRACAO_CELULA_PYTHON.automacoes_exec"
EMAILS_PRINCIPAL = "carlos.lsilva@c6bank.com; sofia.fernandes@c6bank.com"
EMAILS_CC: Optional[str] = None
PASTA_INPUT = (
    Path.home()
    / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A"
    / "Mensageria e Cargas Operacionais - 11.CelulaPython"
    / "graciliano"
    / "automacoes"
    / NOME_AUTOMACAO
    / "arquivos input"
    / NOME_SCRIPT
)
PASTA_LOGS = (
    Path.home()
    / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A"
    / "Mensageria e Cargas Operacionais - 11.CelulaPython"
    / "graciliano"
    / "automacoes"
    / NOME_AUTOMACAO
    / "logs"
    / NOME_SCRIPT
    / DATA_EXEC
)

logger = logging.getLogger(NOME_SCRIPT)
logger.propagate = False
LOG_PATH: Optional[Path] = None


class Execucao:
    def is_servidor(self) -> bool:
        return len(sys.argv) > 1 or "SERVIDOR_ORIGEM" in os.environ or "MODO_EXECUCAO" in os.environ

    def abrir_gui(self) -> Tuple[str, str]:
        from PySide6 import QtWidgets

        app = QtWidgets.QApplication([])
        dialog = QtWidgets.QDialog()
        dialog.setWindowTitle("EXECUCAO")
        layout = QtWidgets.QVBoxLayout()
        label = QtWidgets.QLabel("DIGITE O USUARIO")
        entrada = QtWidgets.QLineEdit()
        layout.addWidget(label)
        layout.addWidget(entrada)
        botoes = QtWidgets.QDialogButtonBox(QtWidgets.QDialogButtonBox.Ok | QtWidgets.QDialogButtonBox.Cancel)
        layout.addWidget(botoes)
        dialog.setLayout(layout)
        modo = "SOLICITACAO"
        usuario = getpass.getuser()
        botoes.accepted.connect(dialog.accept)
        botoes.rejected.connect(dialog.reject)
        if dialog.exec() == QtWidgets.QDialog.Accepted:
            usuario = entrada.text().strip() or usuario
        else:
            modo = "AUTO"
        app.exit()
        return modo, usuario

    def detectar(self) -> Tuple[str, str]:
        if self.is_servidor():
            return "AUTO", getpass.getuser()
        try:
            return self.abrir_gui()
        except Exception:
            return "AUTO", getpass.getuser()


def configurar_logger() -> None:
    global LOG_PATH
    PASTA_LOGS.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(TZ).strftime("%Y%m%d_%H%M%S")
    LOG_PATH = PASTA_LOGS / f"{NOME_SCRIPT}_{timestamp}.log"
    logger.handlers.clear()
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    file_handler = logging.FileHandler(LOG_PATH, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)


def criar_cliente_bq() -> bigquery.Client:
    projeto = BQ_TABELA_DESTINO.split(".")[0]
    return bigquery.Client(project=projeto)


def tratar_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in df.columns:
        serie = df[col]
        if pd.api.types.is_numeric_dtype(serie) or pd.api.types.is_datetime64_any_dtype(serie):
            continue
        serie_convertida = serie.astype("object")
        mascara_validos = serie_convertida.notna()
        serie_convertida.loc[mascara_validos] = serie_convertida.loc[mascara_validos].astype(str).str.upper()
        df[col] = serie_convertida
    colunas_numericas = sum(
        pd.api.types.is_numeric_dtype(df[c]) or pd.api.types.is_datetime64_any_dtype(df[c]) for c in df.columns
    )
    colunas_texto = len(df.columns) - colunas_numericas
    logger.info(
        "tratamento dataframe concluido: %d colunas preservadas, %d colunas texto upper",
        colunas_numericas,
        colunas_texto,
    )
    return df


def gerar_excel(df: pd.DataFrame) -> Path:
    destino_base = Path.home() / "Downloads" / "registro_automacoes"
    destino_base.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(TZ).strftime("%Y%m%d_%H%M%S")
    xlsx_path = destino_base / f"registro_automacoes_{timestamp}.xlsx"
    with pd.ExcelWriter(xlsx_path, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="dados")
    return xlsx_path


def mover_para_logs(arquivo: Path) -> Path:
    destino = PASTA_LOGS / arquivo.name
    if destino.exists():
        timestamp = datetime.now(TZ).strftime("%Y%m%d_%H%M%S")
        destino = destino.with_name(f"{destino.stem}_{timestamp}{destino.suffix}")
    shutil.move(str(arquivo), destino)
    return destino


def preparar_assunto(status: str) -> str:
    return f"CÉLULA PYTHON MONITORAÇÃO - {NOME_SCRIPT} - {status}"


def preparar_corpo_email(
    status: str,
    linhas_processadas: int,
    linhas_inseridas: int,
    linhas_ignoradas: int,
    inicio: datetime,
    fim: datetime,
    motivo_sem_dados: Optional[str] = None,
) -> str:
    tempo_exec = fim - inicio
    tempo_formatado = str(timedelta(seconds=int(tempo_exec.total_seconds())))
    hora_inicio = inicio.astimezone(TZ).strftime("%H:%M:%S")
    hora_fim = fim.astimezone(TZ).strftime("%H:%M:%S")
    motivo = f"<p>DETALHE: {motivo_sem_dados}</p>" if motivo_sem_dados else ""
    corpo = f"""
    <html>
        <body style="font-family: Montserrat, Arial, sans-serif; text-transform: uppercase;">
            <p>AUTOMACAO: {NOME_AUTOMACAO}</p>
            <p>SCRIPT: {NOME_SCRIPT}</p>
            <p>STATUS: {status}</p>
            <p>HORA INICIO: {hora_inicio}</p>
            <p>HORA FIM: {hora_fim}</p>
            <p>TEMPO EXECUCAO: {tempo_formatado}</p>
            <p>LINHAS PROCESSADAS: {linhas_processadas}</p>
            <p>LINHAS INSERIDAS: {linhas_inseridas}</p>
            <p>LINHAS IGNORADAS (DUPLICADAS): {linhas_ignoradas}</p>
            {motivo}
        </body>
    </html>
    """
    return " ".join(corpo.split())


def anexos_email(extra: Optional[List[Path]] = None) -> List[Path]:
    anexos: List[Path] = []
    if LOG_PATH and LOG_PATH.exists():
        anexos.append(LOG_PATH)
    if extra:
        for item in extra:
            if item and item.exists():
                anexos.append(item)
    return anexos


def enviar_email(
    status: str,
    linhas_processadas: int,
    linhas_inseridas: int,
    linhas_ignoradas: int,
    inicio: datetime,
    fim: datetime,
    anexos: Optional[List[Path]] = None,
    motivo_sem_dados: Optional[str] = None,
) -> None:
    try:
        import pythoncom
        from win32com.client import Dispatch
    except Exception:
        logger.warning("bibliotecas de email indisponiveis")
        return
    try:
        pythoncom.CoInitialize()
    except Exception:
        pass
    try:
        outlook = Dispatch("Outlook.Application")
    except Exception:
        logger.warning("outlook indisponivel")
        return
    mail = outlook.CreateItem(0)
    mail.To = EMAILS_PRINCIPAL
    if status == "SUCESSO" and EMAILS_CC:
        mail.CC = EMAILS_CC
    mail.Subject = preparar_assunto(status)
    mail.HTMLBody = preparar_corpo_email(
        status,
        linhas_processadas,
        linhas_inseridas,
        linhas_ignoradas,
        inicio,
        fim,
        motivo_sem_dados,
    )
    for anexo in anexos_email(anexos):
        try:
            mail.Attachments.Add(str(anexo))
        except Exception:
            logger.warning("nao consegui anexar: %s", anexo)
    try:
        mail.Send()
        logger.info("email enviado")
    except Exception:
        logger.warning("falha ao enviar email")


def registrar_metricas(status: str, modo_execucao: str, usuario: str, tempo_exec: str) -> None:
    try:
        client = bigquery.Client(project=BQ_TABELA_METRICAS.split(".")[0])
    except Exception:
        logger.warning("nao foi possivel criar cliente bq para metricas")
        return
    tabela = BQ_TABELA_METRICAS
    schema = [
        bigquery.SchemaField("nome_automacao", "STRING"),
        bigquery.SchemaField("metodo_automacao", "STRING"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("modo_execucao", "STRING"),
        bigquery.SchemaField("tempo_exec", "STRING"),
        bigquery.SchemaField("data_exec", "STRING"),
        bigquery.SchemaField("hora_exec", "STRING"),
        bigquery.SchemaField("usuario", "STRING"),
        bigquery.SchemaField("log_completo", "STRING"),
        bigquery.SchemaField("execucao_do_dia", "STRING"),
        bigquery.SchemaField("observacao", "STRING"),
        bigquery.SchemaField("tabela_referencia", "STRING"),
    ]
    job_config = bigquery.LoadJobConfig(schema=schema, write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
    linhas = [
        {
            "nome_automacao": NOME_AUTOMACAO,
            "metodo_automacao": NOME_SCRIPT,
            "status": status,
            "modo_execucao": modo_execucao,
            "tempo_exec": tempo_exec,
            "data_exec": DATA_EXEC,
            "hora_exec": HORA_EXEC,
            "usuario": f"{usuario}@c6bank.com" if usuario else "",
            "log_completo": None,
            "execucao_do_dia": None,
            "observacao": None,
            "tabela_referencia": None,
        }
    ]
    try:
        load_job = client.load_table_from_json(linhas, tabela, job_config=job_config)
        load_job.result()
        logger.info("metricas registradas: job %s", load_job.job_id)
    except Exception:
        logger.warning("falha ao registrar metricas")


def executar() -> int:
    execucao = Execucao()
    modo_execucao, usuario = execucao.detectar()
    inicio_exec = datetime.now(timezone.utc)
    linhas_processadas = 0
    linhas_inseridas = 0
    linhas_ignoradas = 0
    status = "FALHA"
    excel_movido: Optional[Path] = None
    try:
        logger.info("iniciando exportacao: %s", BQ_TABELA_DESTINO)
        cliente_bq = criar_cliente_bq()
        df = cliente_bq.query(f"SELECT * FROM `{BQ_TABELA_DESTINO}`").result().to_dataframe(create_bqstorage_client=False)
        if df.empty:
            status = "SEM DADOS PARA PROCESSAR"
            enviar_email(
                status,
                0,
                0,
                0,
                inicio_exec,
                datetime.now(timezone.utc),
                [],
                f"PASTA VERIFICADA: {PASTA_INPUT} E TABELA {BQ_TABELA_DESTINO} SEM REGISTROS",
            )
            return RETCODE_SEMDADOSPARAPROCESSAR
        df_tratado = tratar_dataframe(df)
        linhas_processadas = len(df_tratado)
        xlsx_path = gerar_excel(df_tratado)
        excel_movido = mover_para_logs(xlsx_path)
        status = "SUCESSO"
        enviar_email(
            status,
            linhas_processadas,
            linhas_inseridas,
            linhas_ignoradas,
            inicio_exec,
            datetime.now(timezone.utc),
            [excel_movido],
        )
        return RETCODE_SUCESSO
    except Exception:
        logger.exception("falha geral na exportacao")
        status = "FALHA"
        enviar_email(
            status,
            linhas_processadas,
            linhas_inseridas,
            linhas_ignoradas,
            inicio_exec,
            datetime.now(timezone.utc),
            [p for p in [excel_movido] if p],
        )
        return RETCODE_FALHA
    finally:
        fim_exec = datetime.now(timezone.utc)
        tempo_exec = fim_exec - inicio_exec
        horas = int(tempo_exec.total_seconds() // 3600)
        minutos = int((tempo_exec.total_seconds() % 3600) // 60)
        segundos = int(tempo_exec.total_seconds() % 60)
        tempo_formatado = f"{horas:02d}:{minutos:02d}:{segundos:02d}"
        registrar_metricas(status, modo_execucao, usuario, tempo_formatado)


def main() -> int:
    configurar_logger()
    return executar()


if __name__ == "__main__":
    sys.exit(main())
