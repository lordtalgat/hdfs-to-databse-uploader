import kz.talgat.extensions.databases.DataBase;
import kz.dmc.packages.controllers.DMCController;

public class DataBaseSubmitter {
    public static void main(String[] args) {
        try {
            DMCController.get(DataBaseSubmitter.class)
                    .setParams(args[0])
                    .startWork();

            DataBase.runJob();


            if (DMCController.get().isExistAppParam("date")) {
                DMCController.get().setNextIncrementParam("date");
                DMCController.get().createValidationInfo("date");
            } else {
                DMCController.get().createValidationInfo();
            }

            DMCController.get().finishWork();

        } catch (Exception e) {
            try {
                DMCController.get().errorWork(e);
            } catch (Exception e1) {
                e1.printStackTrace();
            }
        }
    }
}
