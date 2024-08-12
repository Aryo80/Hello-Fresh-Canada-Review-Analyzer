
import mlflow
import mlflow.sklearn
from prefect import task
from sklearn.svm import SVC
from sklearn.model_selection import GridSearchCV
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, mean_squared_error, confusion_matrix, precision_recall_curve, roc_curve,classification_report
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import label_binarize
import seaborn as sns
import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import os 

@task
def predict_current_data_batch(current,month):
    mlflow.set_experiment("batch_analysis")
    # Start a new MLflow run and log metrics
    current_batch = current[current['month'] == month]
    X, y, vectorizer = vectorize_data(current_batch)  
    
    with mlflow.start_run(run_name=month, description="Evaluating model on random simulated data"):
        mlflow.set_experiment("hello-fresh-canada")
        # Load the registered model
        model_name = "model_rf"
        model_version = 1
        model_uri = f"models:/{model_name}/{model_version}"
        loaded_model = mlflow.sklearn.load_model(model_uri)
        
        # Run predictions using the loaded model
        predictions = loaded_model.predict(X)
        
        # Calculate accuracy
        accuracy = accuracy_score(y, predictions)
        mlflow.log_metric("accuracy", accuracy)

        # Generate and save artifacts
        # Confusion Matrix
        conf_matrix = confusion_matrix(y, predictions)
        plt.figure(figsize=(10, 6))
        plt.imshow(conf_matrix, interpolation='nearest', cmap=plt.cm.Blues)
        plt.title('Confusion Matrix')
        plt.colorbar()
        plt.xlabel('Predicted label')
        plt.ylabel('True label')
        plt.savefig('training_confusion_matrix.png')
        mlflow.log_artifact('training_confusion_matrix.png')

        # Precision-Recall and ROC Curves
        y_bin = label_binarize(y, classes=np.unique(y))
        y_proba = loaded_model.predict_proba(X)
        n_classes = y_bin.shape[1]

        for i in range(n_classes):
            precision, recall, _ = precision_recall_curve(y_bin[:, i], y_proba[:, i])
            plt.figure(figsize=(10, 6))
            plt.plot(recall, precision, marker='.')
            plt.title(f'Precision-Recall Curve for class {i}')
            plt.xlabel('Recall')
            plt.ylabel('Precision')
            plt.savefig(f'training_precision_recall_curve_class_{i}.png')
            mlflow.log_artifact(f'training_precision_recall_curve_class_{i}.png')

            fpr, tpr, _ = roc_curve(y_bin[:, i], y_proba[:, i])
            plt.figure(figsize=(10, 6))
            plt.plot(fpr, tpr, marker='.')
            plt.title(f'ROC Curve for class {i}')
            plt.xlabel('False Positive Rate')
            plt.ylabel('True Positive Rate')
            plt.savefig(f'training_roc_curve_class_{i}.png')
            mlflow.log_artifact(f'training_roc_curve_class_{i}.png')
        # Remove all PNG files in the current directory
        for file in os.listdir('.'):
            if file.endswith(".png"):
                os.remove(file)
        print(f"Logged Accuracy: {accuracy}")
    return current





@task
def train_evaluate_rf_model(X, y, df):
    # Step 1: Split the data
    X_train = X[0:15000]
    y_train = y[0:15000]
    X_test =  X[15000:]
    y_test =  y[15000:]
    # Step 2: Initialize the Random Forest model

    rf_model = RandomForestClassifier(n_estimators=100, random_state=42)
    
    
    #mlflow.sklearn.autolog()

    # Step 4: Train the model
    with mlflow.start_run():
        rf_model.fit(X_train, y_train)
        
        # Predict on the training set
        pred_train = rf_model.predict(X_train)
        # Predict on the testing set
        pred_test = rf_model.predict(X_test)
        
        # Log metrics
        accuracy = accuracy_score(y_test, pred_test)
        mlflow.log_metric("accuracy", accuracy)
        
        print("RandomForest Model Evaluation")
        print(classification_report(y_test, pred_test))
        
        # Save and log confusion matrix
        rf_cm = confusion_matrix(y_test, pred_test, labels=rf_model.classes_)
        
        # Log confusion matrix
        cm_fig, ax = plt.subplots(figsize=(8, 6))
        sns.heatmap(rf_cm, annot=True, fmt='d', ax=ax, cmap='Blues')
        ax.set_xlabel('Predicted labels')
        ax.set_ylabel('True labels')
        ax.set_title('Random Forest Confusion Matrix')
        cm_path = "rf_confusion_matrix.png"
        plt.savefig(cm_path)
        plt.close()
        mlflow.log_artifact(cm_path)
        # Log the model
        mlflow.sklearn.log_model(rf_model, "model")
    # Update the DataFrame outside the MLflow run
    
    # For training predictions
    # For training predictions
    #df.iloc[:3000, df.columns.get_loc('prediction')] = pred_train
    
    # For testing predictions
    #df.iloc[3000:, df.columns.get_loc('prediction')] = pred_test
    mlflow.end_run()
    return rf_model, rf_cm

def vectorize_data(df):
    
    X = df['text']
    y = df['compound_category']

    vectorizer = TfidfVectorizer(max_features=1000)
    X = vectorizer.fit_transform(X)

    return X, y, vectorizer

@task
def train_tune_svm_model(X,y):
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    param_grid = {
        'C': [ 1],
        'kernel': [ 'rbf'],
        'gamma': [ 'auto']
    }
    svm_model = SVC(probability=True, random_state=42)
    grid_search = GridSearchCV(estimator=svm_model, param_grid=param_grid, cv=3, scoring='accuracy')
    grid_search.fit(X_train, y_train)
    svm_model_best = grid_search.best_estimator_

    svm_y_pred = svm_model_best.predict(X_test)
    print("SVM Model Evaluation")
    print(classification_report(y_test, svm_y_pred))

    # Save confusion matrix
    svm_cm = confusion_matrix(y_test, svm_y_pred, labels=svm_model_best.classes_)
    return svm_model_best, svm_cm




@task
def train_evaluate_svm_model(X, y):
    # Step 1: Split the data
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    mlflow.sklearn.autolog()
    with mlflow.start_run():
        # Step 2: Initialize the SVM model with default parameters
        svm_model = SVC(probability=True, random_state=42)
        
        # Step 3: Train the model
        svm_model.fit(X_train, y_train)
        
        # Step 4: Predict on the test set
        svm_y_pred = svm_model.predict(X_test)
        
        # Log metrics
        accuracy = accuracy_score(y_test, svm_y_pred)
        mlflow.log_metric("accuracy", accuracy)
       
       
        # Evaluate the model
        print("SVM Model Evaluation")
        print(classification_report(y_test, svm_y_pred))
        
        # Save confusion matrix
        svm_cm = confusion_matrix(y_test, svm_y_pred, labels=svm_model.classes_)
        
        return svm_model, svm_cm
