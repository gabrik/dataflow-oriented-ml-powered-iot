from zenoh_flow.interfaces import Operator
from zenoh_flow import Input, Output
from zenoh_flow.types import Context
from typing import Dict, Any
import datetime
import pickle
import json
import logging
import asyncio
import random
import numpy as np
import sklearn_json as skljson
from joblib import parallel_backend
from sklearn import model_selection
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import SGDClassifier
from sklearn.metrics import matthews_corrcoef
from sklearn.model_selection import GridSearchCV
from sklearn.neighbors import KNeighborsClassifier
from sklearn.neural_network import MLPClassifier
from sklearn.pipeline import Pipeline
from sklearn.svm import SVC
from sklearn.tree import DecisionTreeClassifier

DEFAULT_SEED = 7


# This is just forwardning, not sure it is actually needed.
class Trainer(Operator):
    def __init__(
        self,
        context: Context,
        configuration: Dict[str, Any],
        inputs: Dict[str, Input],
        outputs: Dict[str, Output],
    ):
        logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s', level=logging.DEBUG)

        self.configuration = configuration if configuration is not None else {}
        self.output = outputs.get("test_model", None)
        self.store_output = outputs.get("ml_storager", None)
        self.in_stream = inputs.get("train", None)

        if self.in_stream is None:
            raise ValueError("No input 'train' found")
        if self.output is None:
            raise ValueError("No output 'test_model' found")

        if self.store_output is None:
            raise ValueError("No output 'ml_storager' found")

        seed = self.configuration.get("seed", DEFAULT_SEED)
        random.seed(seed)
        np.random.seed(seed)

        self.random_state = seed
        self.X_train = []
        self.y_train = []
        self.best_performance = -1
        clf_lr = SGDClassifier(loss='log_loss', penalty='l2', random_state=self.random_state)
        clf_svm_linear = SGDClassifier(loss='hinge', penalty='l2', random_state=self.random_state)
        clf_knn_3 = KNeighborsClassifier(n_neighbors=3)
        clf_knn_5 = KNeighborsClassifier(n_neighbors=5)
        clf_dt = DecisionTreeClassifier(random_state=self.random_state)
        clf_svm_rbf = SVC(random_state=self.random_state)
        clf_ann = MLPClassifier(activation='relu', solver='adam', random_state=self.random_state, verbose=False)
        self.mutex = asyncio.Lock()

        self.pipeline = Pipeline([('classifier', clf_lr)]) # simple initialization,
                                            # the searchs runs the other classifiers
        self.params = [{'classifier' : [clf_lr]}, {"classifier" : [clf_svm_linear]}, {"classifier" : [clf_svm_linear]},
                        {"classifier" : [clf_knn_3]}, {"classifier" : [clf_knn_5]}, {"classifier" : [clf_dt]},
                        {"classifier" : [clf_svm_rbf]}, {"classifier" : [clf_ann]}]

        self.models = [
            ('LR',  clf_lr), ('SVM_linear', clf_svm_linear),
            ('KNN(3)', clf_knn_3), ('KNN(5)', clf_knn_5),
            ('DT',  clf_dt), ('ANN', clf_ann),
            ('SVM_rbf', clf_svm_rbf),
            ('RF',  RandomForestClassifier(random_state=self.random_state)),
        ]

    def finalize(self) -> None:
        return None

    async def insert_data(self, data: list):
        await self.mutex.acquire()
        logging.debug("[Trainer ]Training on new data")
        self.X_train += data[0]
        self.y_train += data[1]
        # print(f'X_Train: {self.X_train[:10]}\n')
        # print(f'Y_Train: {self.y_train[:10]}\n')
        await self.train_models()
        self.mutex.release()

    async def train_models(self):

        # WARNING, setting n_jobs to anything different than 1 makes the trainer to crash when
        # running it inside a zenoh-flow runtime, still from htop it seems to use all CPUs
        grid = GridSearchCV(self.pipeline, self.params, n_jobs=1, cv=5, scoring=predict, error_score=0)

        # print(f'Pipeline: {pickle.dumps(self.pipeline)}')
        # print(f'Params: { pickle.dumps(self.params)}')
        # print(f'Fn predict: { pickle.dumps(predict)}')
        # print(f'X_Train: { pickle.dumps(self.X_train)[:10] }')
        # print(f'Y_Train: { pickle.dumps(self.y_train)[:10] }')

        grid.fit(self.X_train, self.y_train)
        clf = grid.best_params_["classifier"].__class__.__name__
        msg = {"timestamp" : str(datetime.datetime.now()), "best_mcc" : grid.best_score_, 'model': clf}
        await self.store_output.send(json.dumps(msg).encode("utf-8"))
        logging.debug("[Trainer] sent data to store")
        if grid.best_score_ > self.best_performance:
            logging.debug(f"[Trainer ]New best model with performance {grid.best_score_}, model is {clf}")
            with parallel_backend('loky'):
                model = grid.best_params_["classifier"].fit(self.X_train, self.y_train)

            s = pickle.dumps(model)
            await self.output.send(s)
            logging.debug("[Trainer] sent data to eval")
            self.best_performance = grid.best_score_

    async def iteration(self) -> None:
        data_msg = await self.in_stream.recv()
        payload = json.loads(data_msg.data.decode("utf-8"))

        if "shutdown" in payload:
            shutdown_msg = json.dumps({"shutdown": True, "timestamp": str(datetime.datetime.now())}).encode("utf-8")
            await self.store_output.send(shutdown_msg)
            logging.debug("[Trainer] sent shutdown")
        else:
            await self.insert_data(payload["data"])

        return None


def predict(model, X, y):
    pred = model.predict(X)

    return matthews_corrcoef(y, pred)


def register():
    return Trainer
