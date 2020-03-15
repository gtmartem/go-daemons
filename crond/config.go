package crond


import (
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
)


type cronConfig struct {
	Cron		string	`yaml:"cron,omitempty"`
}


// NewConfig returns new cronConfig with default values
func newConfig() *cronConfig {
	return &cronConfig{
		Cron: "* * * * *", // starts every minute
	}
}


// getConfig reads yml file and unmarshal it to cronConfig structure
func getConfig(pathToConfig string) (config *cronConfig) {
	config = newConfig()
	configFile, err := ioutil.ReadFile(pathToConfig)
	if err != nil {
		log.Printf("%s reading err: %v", pathToConfig, err)
		return config
	}
	err = yaml.Unmarshal(configFile, config)
	if err != nil {
		log.Fatalf("unmarshal err: %v", err)
	}
	return
}
