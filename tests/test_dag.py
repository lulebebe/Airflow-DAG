import os
from airflow.models import DagBag
from airflow.utils.dag_cycle_tester import check_cycle


def test_dag_loading():
    """Validar que los DAGs carguen sin errores de sintaxis o importación."""
    dag_folder = os.path.join(os.path.dirname(__file__), '../dags')

    dag_bag = DagBag(dag_folder=dag_folder, include_examples=False)

    # Ignorar errores causados por ciclos — eso es responsabilidad de test_dag_cycles
    non_cycle_errors = {
        filepath: error
        for filepath, error in dag_bag.import_errors.items()
        if "Cycle detected" not in error and "AirflowDagCycleException" not in error
    }

    assert len(non_cycle_errors) == 0, f"Errores de importación encontrados: {non_cycle_errors}"
    assert len(dag_bag.dags) > 0, "No se encontraron DAGs en la carpeta indicada"


def test_dag_cycles():
    """Validar que NO existen ciclos en los DAGs."""
    dag_folder = os.path.join(os.path.dirname(__file__), '../dags')
    dag_bag = DagBag(dag_folder=dag_folder, include_examples=False)

    # Verificar ciclos en los DAGs que cargaron correctamente
    for dag_id, dag in dag_bag.dags.items():
        check_cycle(dag)

    # Airflow detecta ciclos en tiempo de carga y los registra en import_errors.
    # Este bloque atrapa esos casos para que el fallo aparezca en este test.
    cycle_errors = {
        filepath: error
        for filepath, error in dag_bag.import_errors.items()
        if "Cycle detected" in error or "AirflowDagCycleException" in error
    }

    assert len(cycle_errors) == 0, f"Ciclos detectados en los siguientes DAGs: {cycle_errors}"


def test_specific_dag_specs():
    """Validar propiedades especificas del DAG de ejemplo."""
    dag_folder = os.path.join(os.path.dirname(__file__), '../dags')
    dag_bag = DagBag(dag_folder=dag_folder, include_examples=False)

    dag_id = 'example_ci_cd_dag'
    # Usar .dags.get() en lugar de .get_dag() para evitar consultas a la BD
    dag = dag_bag.dags.get(dag_id)

    assert dag is not None, f"El DAG {dag_id} no fue encontrado"
    assert len(dag.tasks) == 1
    assert 'ci-cd' in dag.tags
    assert dag.catchup is False
