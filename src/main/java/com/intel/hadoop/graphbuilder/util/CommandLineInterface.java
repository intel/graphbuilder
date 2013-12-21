/**
 * Copyright (C) 2013 Intel Corporation.
 *     All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * For more about this software visit:
 *     http://www.01.org/GraphBuilder
 */
package com.intel.hadoop.graphbuilder.util;

import com.intel.hadoop.graphbuilder.pipeline.GraphConstructionPipeline;
import org.apache.commons.cli.*;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * A general command line parsing utility for graph builder. This CLI class is a wrapper around the 
 * GenericOptionsParser and the apache commons cli PosixParser. It manages and centralizes much of 
 * the error checking and parser instantiation to one class versus distributing across all the demo apps.
 *
 * Uses the Hadoop generic options parser to parse config files.
 * The reserved options: -conf, -D, -fs, -jt, -files, -libjars, and -archives are already used by the 
 * Hadoop generic options parser. Don't use any of the reserved options to avoid conflicts. All the 
 * Hadoop generic options must be placed before any app specific options.
 *
 * Usage:
 * <p>
 *     <code>-conf path/to/config/file</code>   Specifies the configuration file.
 *     <code>-DmySingleConfigName=mySingleConfigValue</code> Specifies the individual config property.
 *     or
 *     <code>-D mySingleConfigName=mySingleConfigValue</code> Specifies the individual config property.
 * </p>
 * @see GenericOptionsParser
 * @see PosixParser
 */
public class CommandLineInterface{

    private static final Logger  LOG           = Logger.getLogger(CommandLineInterface.class);
    private static final String  GENERIC_ERROR = "Error parsing options";
    private static final Option HELP_OPTION = OptionBuilder.withLongOpt("help").withDescription("").create("h");
    private Options              options       = new Options();
    private CommandLine          cmd           = null;
    private RuntimeConfig        runtimeConfig = RuntimeConfig.getInstance();
    private GenericOptionsParser genericOptionsParser;

    /**
     * A wrapper to the regular {@code hasOption} command line class.
     * Checks for a given command line option.
     * @param {@code option}  The name of the option being requested.
     * @return  true  If and only if the command line has the option.
     */
    public boolean hasOption(String option) {
        return cmd.hasOption(option);
    }

    /**
     * A wrapper to the regular {@code getOptionValue} command line class.
     * Gets the value of the option from the command line.
     * @param {@code option} The name of the option whose value is requested.
     * @return value The value of the option as specified by the command line.
     */
    public String getOptionValue(String option) {
        return cmd.getOptionValue(option);
    }


    /**
     * A simple wrapper to reduce the length of the call in the demo app.
     * @param {@code job} The {@code GraphConstructionPipeline(hadoop)} job to which we will attach our config.
     * @return The same {@code GraphConstructionPipeline(hadoop)} job with our config.
     */
    public GraphConstructionPipeline addConfig(GraphConstructionPipeline job){
        return this.getRuntimeConfig().addConfig(job);
    }

    /**
     * Parses the raw arguments into a {@code CommandLine} object.
     * @param {@code args} The raw command line arguments as a string array.
     * @return  A nicely packaged {@code CommandLine} object.
     */
    public CommandLine parseArgs(String[] args) {

        //send the command line options through the hadoop parser the config options first

        try {
            genericOptionsParser = new GenericOptionsParser(args);
        } catch (IOException e) {
            // show help and terminate the process
            showHelp("Error parsing hadoop generic options.");
        }

        //make sure the config file exist when it's specified
        if(genericOptionsParser.getCommandLine().hasOption("conf") &&
                !new File(genericOptionsParser.getCommandLine().getOptionValue("conf")).exists()){
            GraphBuilderExit.graphbuilderFatalExitNoException(StatusCode.CANNOT_FIND_CONFIG_FILE,
                    "Configuration file " + genericOptionsParser.getCommandLine().getOptionValue("conf") +
                            " cannot be found.", LOG);
        }

        //load all the grahpbuilder configs into the runtime class

        runtimeConfig.loadConfig(genericOptionsParser.getConfiguration());

        //parse the remaining args

        CommandLineParser parser = new PosixParser();

        try {
            cmd = parser.parse(options, genericOptionsParser.getRemainingArgs());
        }
        catch (ParseException e){
            if(e instanceof UnrecognizedOptionException){
                showErrorUnrecognizedOption(getUnrecognizedOptionFromException(e));

            }else if(e instanceof MissingOptionException){
                showErrorOption(getFirstMissingOptionFromException(e));

            }else if(e instanceof MissingArgumentException){
                showErrorMissingArgument(getMissingArgumentFromException(e));

            } else {
                showHelp("Error parsing option string.");
            }
        }
        return cmd;
    }

    /**
     * Makes sure that all of the required options are present in the raw arguments.
     * @param {@code args}  The raw arguments as a string array.
     */
    public CommandLine checkCli(String[] args) {
        CommandLine cmd = parseArgs(args);

        if (cmd == null) {
            showHelp("Error parsing command line options");
            GraphBuilderExit.graphbuilderFatalExitNoException(StatusCode.BAD_COMMAND_LINE,
                    "Error parsing command line options", LOG);
        }

        if(cmd.hasOption(HELP_OPTION.getOpt())){
            showHelp("Help1");
        }

        //report any missing required options
        List<String> opts = options.getRequiredOptions();
        for(String option: opts){
            if (!cmd.hasOption(option)) {
                showErrorOption(option);
            }
        }

        //report parsed values for options given
        showOptionsParsed();
        return cmd;
    }

    /**
     * Gets Hadoop's generic options parser.
     * @return  Hadoop's generic options parser.
     */
    public GenericOptionsParser getGenericOptionsParser() {
        return genericOptionsParser;
    }

    /**
     * Displays the parsed options for the given option name.
     * @param {@code option} The name of option as a string.
     */
    public void showParsedOption(Option option){
        String message;
        if(option.hasArg()){
            message = String.format("Parsed -%s -%s:\t %s", option.getOpt(),
                    option.getLongOpt(), cmd.getOptionValue(option.getOpt()) );
        }else{
            message = String.format("Parsed -%s -%s:\t %b", option.getOpt(),
                    option.getLongOpt(), cmd.hasOption(option.getOpt()) );
        }
        LOG.debug(message);
    }

    /**
     * Displays the parsed options.
     */
    public void showOptionsParsed(){
        Iterator<Option> optionIterator = options.getOptions().iterator();
        while(optionIterator.hasNext()){
            Option nextOption = optionIterator.next();
            if (cmd.hasOption(nextOption.getOpt())) {
                showParsedOption(nextOption);
            }
        }
    }

    /**
     * Displays a help message when a user sets the help option.
     * @param {@code message}  The error message to display.
     */
    public void showHelp(String message){
        _showHelp(message);
    }

    /**
     * Displays a help message after a bad command line param.
     * @param {@code message} The error message to display on the command line.
     */
    public void showError(String message){
        _showError(message);
    }

    private void showErrorMissingArgument(String option){
        String error = GENERIC_ERROR;
        if( option != null){
            error = String.format("Option -%s --%s %s is missing it's argument", options.getOption(option).getOpt(),
                    options.getOption(option).getLongOpt(), options.getOption(option).getDescription());
        }
        _showError(error);
    }

    private void showErrorUnrecognizedOption(String option){
        String error = GENERIC_ERROR;
        if( option != null){
            error = String.format("Option -%s not recognized", option);
        }
        _showError(error);
    }

    private void showErrorOption(String option) {
        String error = GENERIC_ERROR;
        if( option != null){
            //show the short, long option and the description for the missing option
            error = String.format("Option -%s --%s %s is missing", options.getOption(option).getOpt(),
                    options.getOption(option).getLongOpt(), options.getOption(option).getDescription());
        }
        _showError(error);
    }

    private void _showError(String error){
        if(error == null || error.trim().isEmpty()){
            error = " ";
        }
        HelpFormatter h = new HelpFormatter();
        h.printHelp(error, options);
        GraphBuilderExit.graphbuilderFatalExitNoException(StatusCode.BAD_COMMAND_LINE,
                "GRAPHBUILDER_ERROR: Unable to process command line.", LOG);
    }

    private void _showHelp(String help){
        if(help == null || help.trim().length() > 0){
            help = " ";
        }
        HelpFormatter h = new HelpFormatter();
        h.printHelp(help, options);
        GraphBuilderExit.graphbuilderExitNoException(StatusCode.SUCCESS);
    }

    public void setOptions(Options options) {
        this.options = options;
        this.options.addOption(HELP_OPTION);
    }

    public void removeOptions() {
        this.options = null;
    }

    public void setOption(Option option) {
        this.options.addOption(option);
    }


    public RuntimeConfig getRuntimeConfig() {
        return runtimeConfig;
    }

    public Options getOptions() {
        return options;
    }

    public CommandLine getCmd() {
        return cmd;
    }

    /**
     * Checks if the lack of an option caused a parsing exception.
     * @param {@code e}       The parse exception that was thrown.
     * @param {@code option}  The option that should be in the {@code MissingOptionException}.
     * @return A boolean indicating weather or not the {@code String} option is the missing option for which we are looking.
     */
    public static boolean lookForOptionException(ParseException e, String option) {

        MissingOptionException missingOptions;
        if (e instanceof MissingOptionException) {
            missingOptions = (MissingOptionException) e;
        } else {
            return false;
        }

        for (int index = 0; index < missingOptions.getMissingOptions().size(); index++) {

            String checkOption = (String) missingOptions.getMissingOptions().get(index);

            if (checkOption.equals(option)) return true;
        }
        return false;
    }

    /**
     * Converts the missing argument exception into a string message.
     * @param {@code ex} A {@code ParseException}.
     */
    public static String getMissingArgumentFromException(ParseException ex){
        MissingArgumentException missingArgumentException;

        if (ex instanceof MissingArgumentException) {
            missingArgumentException = (MissingArgumentException) ex;
        } else {
            return null;
        }

        if(missingArgumentException.getOption() != null ){
            return missingArgumentException.getOption().getOpt();
        } else {
            return null;
        }
    }

    /**
     * Checks if an unrecognized option caused a parsing exception.
     *
     * @param {@code ex} The parsing exception.
     * @return The name of the unrecognized option.
     */
    public static String getUnrecognizedOptionFromException(ParseException ex){
        UnrecognizedOptionException unrecognizedOption;

        if (ex instanceof UnrecognizedOptionException) {
            unrecognizedOption = (UnrecognizedOptionException) ex;
        } else {
            return null;
        }

        if(unrecognizedOption.getOption() != null ){
            return unrecognizedOption.getOption();
        } else {
            return null;
        }
    }

    /**
     * Finds the first missing option from a parsing exception.
     * @param {@code ex} The parsing exception.
     * @return  The name of the first missing option.
     */
    public static String getFirstMissingOptionFromException(ParseException ex){
        MissingOptionException missingOptions;

        if (ex instanceof MissingOptionException) {
            missingOptions = (MissingOptionException) ex;
        } else {
            return null;
        }

        if(missingOptions.getMissingOptions().size() > 0 ){
            return (String) missingOptions.getMissingOptions().get(0);
        }
        else{
            return null;
        }

    }
}
